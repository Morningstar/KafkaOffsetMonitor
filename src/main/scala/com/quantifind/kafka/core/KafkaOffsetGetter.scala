package com.quantifind.kafka.core

import java.nio.{BufferUnderflowException, ByteBuffer}
import java.util
import java.util.{Arrays, Properties}

import com.quantifind.kafka.OffsetGetter.OffsetInfo
import com.quantifind.kafka.offsetapp.OffsetGetterArgs
import com.quantifind.kafka.{Node, OffsetGetter}
import com.quantifind.utils.ZkUtilsWrapper
import com.twitter.util.Time
import kafka.admin.AdminClient
import kafka.common.{KafkaException, OffsetAndMetadata, TopicAndPartition}
import kafka.coordinator._
import kafka.utils.Logging
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.consumer._
import org.apache.kafka.common.{PartitionInfo, TopicPartition}

import scala.collection._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Await, Future, duration}


/**
  * Created by rcasey on 11/16/2016.
  */
class KafkaOffsetGetter(zkUtilsWrapper: ZkUtilsWrapper, args: OffsetGetterArgs) extends OffsetGetter {

	import KafkaOffsetGetter._

	// TODO: We will get all data from the Kafka broker in this class.  This is here simply to satisfy
	// the OffsetGetter dependency until it can be refactored.
	override val zkUtils = zkUtilsWrapper

	override def processPartition(group: String, topic: String, partitionId: Int): Option[OffsetInfo] = {

		val topicPartition = new TopicPartition(topic, partitionId)

		committedOffsetMap.get(GroupTopicPartition(group, topicPartition)) map { offsetMetaData =>

			// BIT O'HACK to deal with timing:
			// Due to thread and processing timing, it is possible that the value we have for the topicPartition's
			// logEndOffset has not yet been updated to match the value on the broker.  When this happens, report the
			// topicPartition's logEndOffset as "committedOffset - lag", as the topicPartition's logEndOffset is *AT LEAST*
			// this value
			val logEndOffset: Option[Long] = logEndOffsetsMap.get(topicPartition)
			val committedOffset: Long = offsetMetaData.offset
			val lag: Long = logEndOffset.get - committedOffset
			val logEndOffsetReported: Long = if (lag < 0) committedOffset - lag else logEndOffset.get

			// Get client information if we can find an associated client
			var clientString: Option[String] = Option("NA")
			val filteredClients = clients.filter(c => (c.group == group && c.topicPartitions.contains(topicPartition)))
			if (!filteredClients.isEmpty) {
				val client: ClientGroup = filteredClients.head
				clientString = Option(client.clientId + client.clientHost)
			}

			OffsetInfo(group = group,
				topic = topic,
				partition = partitionId,
				offset = committedOffset,
				logSize = logEndOffsetReported,
				owner = clientString,
				creation = Time.fromMilliseconds(offsetMetaData.commitTimestamp),
				modified = Time.fromMilliseconds(offsetMetaData.expireTimestamp))
		}
	}

	override def getGroups: Seq[String] = {
		topicAndGroups.groupBy(_.group).keySet.toSeq.sorted
	}

	override def getTopicList(group: String): List[String] = {
		topicAndGroups.filter(_.group == group).groupBy(_.topic).keySet.toList.sorted
	}

	override def getTopicMap: Map[String, scala.Seq[String]] = {
		topicAndGroups.groupBy(_.topic).mapValues(_.map(_.group).toSeq)
	}

	override def getActiveTopicMap: Map[String, Seq[String]] = {
		getTopicMap
	}

	override def getTopics: Seq[String] = {
		topicPartitionsMap.keys.toSeq.sorted
	}

	override def getClusterViz: Node = {
		val clusterNodes = topicPartitionsMap.values.map(partition => {
			Node(partition.get(0).leader().host() + ":" + partition.get(0).leader().port(), Seq())
		}).toSet.toSeq.sortWith(_.name < _.name)
		Node("KafkaCluster", clusterNodes)
	}
}

object KafkaOffsetGetter extends Logging {

	val committedOffsetMap: concurrent.Map[GroupTopicPartition, OffsetAndMetadata] = concurrent.TrieMap()
	val logEndOffsetsMap: concurrent.Map[TopicPartition, Long] = concurrent.TrieMap()

	// Swap the object on update
	var activeTopicPartitions: immutable.Set[TopicAndPartition] = immutable.HashSet()
	var clients: immutable.Set[ClientGroup] = immutable.HashSet()
	var topicAndGroups: immutable.Set[TopicAndGroup] = immutable.HashSet()
	var topicPartitionsMap: immutable.Map[String, util.List[PartitionInfo]] = immutable.HashMap()


	private def createNewKafkaConsumer(args: OffsetGetterArgs, group: String, autoCommitOffset: Boolean): KafkaConsumer[Array[Byte], Array[Byte]] = {

		val props: Properties = new Properties
		props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, args.kafkaBrokers)
		props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, args.kafkaSecurityProtocol)
		props.put(ConsumerConfig.GROUP_ID_CONFIG, group)
		props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, if (autoCommitOffset) "true" else "false")
		props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000")
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer")
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer")
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, if (args.kafkaOffsetForceFromStart) "earliest" else "latest")

		new KafkaConsumer[Array[Byte], Array[Byte]](props)
	}

	private def createNewAdminClient(args: OffsetGetterArgs): AdminClient = {

		val sleepAfterFailedAdminClientConnect: Int = 30000
		var adminClient: AdminClient = null

		val props: Properties = new Properties
		props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, args.kafkaBrokers)
		props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, args.kafkaSecurityProtocol)

		while (null == adminClient) {

			try {

				info("Creating new Kafka AdminClient to get consumer and group info.");
				adminClient = AdminClient.create(props)
			}

			catch {

				case e: Throwable =>

					if (null != adminClient) {

						adminClient.close()
						adminClient = null
					}

					val errorMsg = "Error creating an AdminClient.  Will attempt to re-create in %d seconds".format(sleepAfterFailedAdminClientConnect)
					error(errorMsg, e)
					Thread.sleep(sleepAfterFailedAdminClientConnect)
			}
		}

		info("Created admin client: " + adminClient)
		adminClient
	}

	/**
	  * Attempts to parse a kafka message as an offset message.
	  *
	  * @author Robert Casey (rcasey212@gmail.com)
	  * @param message message retrieved from the kafka client's poll() method
	  * @return key-value of GroupTopicPartition and OffsetAndMetadata if the message was a valid offset message,
	  *         otherwise None
	  */
	def tryParseOffsetMessage(message: ConsumerRecord[Array[Byte], Array[Byte]]): Option[(GroupTopicPartition, OffsetAndMetadata)] = {

		try {
			// If the message has a null key, there is nothing that can be done
			if (message.key == null) {

				info("Ignoring message with a null key.")
				return None
			}

			val baseKey: BaseKey = GroupMetadataManager.readMessageKey(ByteBuffer.wrap(message.key))

			// Match on the key to see if the message is an offset message
			baseKey match {

				// This is the type we are looking for
				case b: OffsetKey =>
					val messageBody: Array[Byte] = message.value()

					if (messageBody != null) {

						val gtp: GroupTopicPartition = b.key
						val offsetAndMetadata: OffsetAndMetadata = GroupMetadataManager.readOffsetMessageValue(ByteBuffer.wrap(messageBody))
						return Option(gtp, offsetAndMetadata)
					}
					else {

						// Have run into circumstances where kafka sends an offset message with a good key, but with a
						// null body/value. Return None if the body/value is null, as message is not parsable
						info("Ignoring offset message with NULL body/value.")
						return None
					}

				// Return None for all non-offset messages
				case _ =>
					info("Ignoring non-offset message.")
					return None
			}
		} catch {

			case malformedEx@(_: BufferUnderflowException | _: KafkaException) =>
				val errorMsg = String.format("The message was malformed and does not conform to a type of (BaseKey, OffsetAndMetadata. Ignoring this message.")
				error(errorMsg, malformedEx)
				return None

			case e: Throwable =>
				val errorMsg = String.format("An unhandled exception was thrown while attempting to determine the validity of a message as an offset message. This message will be ignored.")
				error(errorMsg, e)
				return None
		}
	}

	def startCommittedOffsetListener(args: OffsetGetterArgs) = {

		val group: String = "kafka-monitor-committedOffsetListener"
		val consumerOffsetTopic = "__consumer_offsets"
		var offsetConsumer: KafkaConsumer[Array[Byte], Array[Byte]] = null

		while (true) {

			try {

				if (null == offsetConsumer) {

					logger.info("Creating new Kafka Client to get consumer group committed offsets")
					offsetConsumer = createNewKafkaConsumer(args, group, false)

					offsetConsumer.subscribe(
						Arrays.asList(consumerOffsetTopic),
						new ConsumerRebalanceListener {
							override def onPartitionsAssigned(partitions: util.Collection[TopicPartition]) = {

								if (args.kafkaOffsetForceFromStart) {

									val topicPartitionIterator = partitions.iterator()

									while (topicPartitionIterator.hasNext()) {

										val topicPartition: TopicPartition = topicPartitionIterator.next()
										offsetConsumer.seekToBeginning(Arrays.asList(topicPartition))
									}
								}
							}

							override def onPartitionsRevoked(partitions: util.Collection[TopicPartition]) = {
								/* Do nothing for now */
							}
						})
				}

				val messages: ConsumerRecords[Array[Byte], Array[Byte]] = offsetConsumer.poll(500)
				val messageIterator = messages.iterator()

				while (messageIterator.hasNext()) {

					val message: ConsumerRecord[Array[Byte], Array[Byte]] = messageIterator.next()
					val offsetMessage: Option[(GroupTopicPartition, OffsetAndMetadata)] = tryParseOffsetMessage(message)

					if (offsetMessage.isDefined) {

						// Deal with the offset message
						val messageOffsetMap: (GroupTopicPartition, OffsetAndMetadata) = offsetMessage.get
						val gtp: GroupTopicPartition = messageOffsetMap._1
						val offsetAndMetadata: OffsetAndMetadata = messageOffsetMap._2

						// Get current offset for topic-partition
						val existingCommittedOffsetMap: Option[OffsetAndMetadata] = committedOffsetMap.get(gtp)

						// Update committed offset only if the new message brings a change in offset for the topic-partition:
						//   a changed offset for an existing topic-partition, or a new topic-partition
						if (!existingCommittedOffsetMap.isDefined || existingCommittedOffsetMap.get.offset != offsetAndMetadata.offset) {

							val group: String = gtp.group
							val topic: String = gtp.topicPartition.topic
							val partition: Long = gtp.topicPartition.partition
							val offset: Long = offsetAndMetadata.offset

							info(s"Updating committed offset: g:$group,t:$topic,p:$partition: $offset")
							committedOffsetMap += messageOffsetMap
						}
					}
				}
			} catch {

				case e: Throwable => {
					val errorMsg = String.format("An unhandled exception was thrown while reading messages from the committed offsets topic.")
					error(errorMsg, e)

					if (null != offsetConsumer) {

						offsetConsumer.close()
						offsetConsumer = null
					}
				}
			}
		}
	}

	def startAdminClient(args: OffsetGetterArgs) = {

		val sleepOnDataRetrieval: Int = 30000
		val sleepOnError: Int = 60000
		val awaitForResults: Int = 30000
		var adminClient: AdminClient = null

		while (true) {

			try {

				if (null == adminClient) {

					adminClient = createNewAdminClient(args)
				}

				var hasError: Boolean = false

				while (!hasError) {

					lazy val f = Future {

						try {

							val newTopicAndGroups: mutable.Set[TopicAndGroup] = mutable.HashSet()
							val newClients: mutable.Set[ClientGroup] = mutable.HashSet()
							val newActiveTopicPartitions: mutable.HashSet[TopicAndPartition] = mutable.HashSet()

							val groupOverviews = adminClient.listAllConsumerGroupsFlattened()

							groupOverviews.foreach((groupOverview: GroupOverview) => {

								val groupId: String = groupOverview.groupId;
								val consumerGroupSummary: AdminClient#ConsumerGroupSummary= adminClient.describeConsumerGroup(groupId)

								consumerGroupSummary.consumers.getOrElse(List()).foreach((consumerSummary) => {

									val clientId: String = consumerSummary.clientId
									val clientHost: String = consumerSummary.host

									val topicPartitions: List[TopicPartition] = consumerSummary.assignment

									topicPartitions.foreach((topicPartition) => {

										newActiveTopicPartitions += TopicAndPartition(topicPartition.topic(), topicPartition.partition())
										newTopicAndGroups += TopicAndGroup(topicPartition.topic(), groupId)
									})

									newClients += ClientGroup(groupId, clientId, clientHost, topicPartitions.toSet)
								})
							})

							activeTopicPartitions = newActiveTopicPartitions.toSet
							clients = newClients.toSet
							topicAndGroups = newTopicAndGroups.toSet
						}

						catch {

							case e: Throwable =>

								val errorMsg: String = "Kafka AdminClient polling aborted due to an unexpected exception."
								error(errorMsg, e)
								hasError = true
								if (null != adminClient) {

									adminClient.close()
									adminClient = null
								}
						}
					}

					Await.result(f, duration.pairIntToDuration(awaitForResults, duration.MILLISECONDS))

					if (hasError) {
						Thread.sleep(sleepOnError)
					} else {
						Thread.sleep(sleepOnDataRetrieval)
					}
				}
			}

			catch {

				case tex: java.util.concurrent.TimeoutException => {
					warn("The AdminClient timed out.  It will be closed and restarted.", tex)
					if (null != adminClient) {
						adminClient.close()
						adminClient = null
					}
				}
			}
		}
	}

	def startLogEndOffsetGetter(args: OffsetGetterArgs) = {

		val group: String = "kafka-monitor-LogEndOffsetGetter"
		val sleepOnDataRetrieval: Int = 10000
		val sleepOnError: Int = 30000
		var logEndOffsetGetter: KafkaConsumer[Array[Byte], Array[Byte]] = null

		while (true) {

			try {

				while (null == logEndOffsetGetter) {

					logEndOffsetGetter = createNewKafkaConsumer(args, group, false)
				}

				// Get topic-partitions
				topicPartitionsMap = JavaConversions.mapAsScalaMap(logEndOffsetGetter.listTopics()).toMap
				val distinctPartitionInfo: Seq[PartitionInfo] = (topicPartitionsMap.values).flatten(listPartitionInfo => JavaConversions.asScalaBuffer(listPartitionInfo)).toSeq

				// Iterate over each distinct PartitionInfo
				distinctPartitionInfo.foreach(partitionInfo => {

					// Get the LogEndOffset for the TopicPartition
					val topicPartition: TopicPartition = new TopicPartition(partitionInfo.topic, partitionInfo.partition)
					logEndOffsetGetter.assign(Arrays.asList(topicPartition))
					logEndOffsetGetter.seekToEnd(Arrays.asList(topicPartition))
					val logEndOffset: Long = logEndOffsetGetter.position(topicPartition)

					// Update the TopicPartition map with the current LogEndOffset if it exists, else add a new entry to the map
					if (logEndOffsetsMap.contains(topicPartition)) {
						logEndOffsetsMap.update(topicPartition, logEndOffset)
					}
					else {
						logEndOffsetsMap += (topicPartition -> logEndOffset)
					}
				})

				Thread.sleep(sleepOnDataRetrieval)
			}

			catch {

				case e: Throwable => {
					val errorMsg = String.format("The Kafka Client reading topic/partition LogEndOffsets has thrown an unhandled exception.  Will attempt to reconnect.")
					error(errorMsg, e)

					if (null != logEndOffsetGetter) {

						logEndOffsetGetter.close()
						logEndOffsetGetter = null
					}
				}
			}
		}
	}
}

case class TopicAndGroup(topic: String, group: String)

case class ClientGroup(group: String, clientId: String, clientHost: String, topicPartitions: Set[TopicPartition])