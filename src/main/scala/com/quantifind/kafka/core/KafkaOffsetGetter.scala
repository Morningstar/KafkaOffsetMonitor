package com.quantifind.kafka.core

import java.io.FileInputStream
import java.nio.{BufferUnderflowException, ByteBuffer}
import java.util
import java.util.concurrent.atomic.AtomicReference
import java.util.{Arrays, Properties}

import com.morningstar.kafka.KafkaCommittedOffset
import com.morningstar.kafka.KafkaOffsetMetadata
import com.morningstar.kafka.KafkaOffsetStorage
import com.morningstar.kafka.KafkaTopicPartition
import com.morningstar.kafka.KafkaTopicPartitionLogEndOffset
import com.quantifind.kafka.OffsetGetter.OffsetInfo
import com.quantifind.kafka.offsetapp.OffsetGetterArgs
import com.quantifind.kafka.{Node, OffsetGetter}
import com.quantifind.utils.ZkUtilsWrapper
import com.twitter.util.Time
import kafka.common.TopicAndPartition
import kafka.coordinator.group._
import kafka.utils.Logging
import kafka.common.OffsetAndMetadata
import kafka.admin.AdminClient
import kafka.coordinator.group.GroupTopicPartition
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.consumer._
import org.apache.kafka.common.{KafkaException, PartitionInfo, TopicPartition}

import scala.collection.mutable.ArrayBuffer
import scala.collection.{mutable, _}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Await, Future, duration}
import scala.collection.JavaConverters._





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
//		val topicAndPartition = TopicAndPartition(topic, partitionId)
		val optionalOffsetMetaData: Option[OffsetAndMetadata] = committedOffsetMap.get(GroupTopicPartition(group, topicPartition))

		if (!optionalOffsetMetaData.isDefined) {
			error(s"processPartition: Could not find group-topic-partition in committedOffsetsMap, g:$group,t:$topic,p:$partitionId")
			return None
		}

		val offsetMetaData: OffsetAndMetadata = optionalOffsetMetaData.get;
		val isActiveGroup: Boolean = isGroupActive(group)

		kafkaOffsetStorage.addCommittedOffset(
			new KafkaCommittedOffset(
				group,
				isActiveGroup,
				topic,
				partitionId,
				offsetMetaData.offset,
				offsetMetaData.commitTimestamp
			)
		)

		if (!isActiveGroup) {
			info(s"processPartition: Not reporting offset because group is not active, g:$group,t:$topic,p:$partitionId")
			return None
		}

		val logEndOffset: Long = logEndOffsetsMap.get(topicPartition).getOrElse(-1)

		if (logEndOffset == -1) {
			info(s"processPartition: Not reporting this offset as we do not yet have logEndOffset to calc lag, g:$group,t:$topic,p:$partitionId")
			return None
		}

		// BIT O'HACK to deal with timing:
		// Due to thread and processing timing, it is possible that the value we have for the topicPartition's
		// logEndOffset has not yet been updated to match the value on the broker.  When this happens, report the
		// topicPartition's logEndOffset as "committedOffset - lag", as the topicPartition's logEndOffset is *AT LEAST*
		// this value
		val committedOffset: Long = offsetMetaData.offset
		val logEndOffsetReported: Long = if (committedOffset > logEndOffset) committedOffset else logEndOffset

		// Get client information if we can find an associated client
		var clientString: Option[String] = Option("NA")
		val filteredClient = clients.get().filter(c => (c.group == group && c.topicPartitions.contains(topicPartition))).headOption
		if (filteredClient.isDefined) {
			clientString = Option(filteredClient.get.clientId + filteredClient.get.clientHost)
		}

		Some(
			OffsetInfo(group = group,
				topic = topic,
				partition = partitionId,
				offset = committedOffset,
				logSize = logEndOffsetReported,
				owner = clientString,
				creation = Time.fromMilliseconds(offsetMetaData.commitTimestamp),
				modified = Time.fromMilliseconds(offsetMetaData.expireTimestamp))
		)
	}

	override def getGroups: Seq[String] = {
		committedOffsetMap.keys.map(_.group).toSet.toSeq.sorted
		//topicAndGroups.get().groupBy(_.group).keySet.toSeq.sorted
	}

	override def getActiveGroups: Seq[String] = {
		activeTopicAndGroups.get()
			.map(_.group)
			.toSeq
	}

	override def getTopicList(group: String): List[String] = {
		committedOffsetMap.keySet
			.filter(_.group.equals(group))
			.map(_.topicPartition.topic)
			.toSeq
			.toList
		//topicAndGroups.get().filter(_.group == group).groupBy(_.topic).keySet.toList.sorted
	}

	override def getTopicMap: Map[String, scala.Seq[String]] = {
		committedOffsetMap.keySet
			.groupBy(_.topicPartition.topic)
			.mapValues(_.map(_.group).toSeq)

		//topicAndGroups.get().groupBy(_.topic).mapValues(_.map(_.group).toSeq)
	}

	override def getActiveTopicMap: Map[String, Seq[String]] = {
		activeTopicAndGroups.get().groupBy(_.topic).mapValues(_.map(_.group).toSeq)
		//getTopicMap
	}

	override def getTopics: Seq[String] = {
		activeTopicPartitionsMap.get().keys.toSeq.sorted
	}

	override def getTopicsAndLogEndOffsets: Seq[TopicLogEndOffsetInfo] = {
		var topicsAndLogEndOffsets: Set[TopicLogEndOffsetInfo] = mutable.HashSet()

		for (topic: String <- activeTopicPartitionsMap.get().keys) {

			var topicPartitionOffsetSet: Set[TopicPartitionLogEndOffset] = new mutable.HashSet[TopicPartitionLogEndOffset]
			var sumLogEndOffset: Long = 0;

			val topicPartitionOffset: immutable.Map[TopicPartition, Long] = logEndOffsetsMap.filter(p => p._1.topic() == topic).toMap
			for (topicPartition: TopicPartition <- topicPartitionOffset.keys.toSeq.sortWith(_.partition() > _.partition())) {

				val partition: Int = topicPartition.partition()
				val logEndOffset: Long = logEndOffsetsMap.get(topicPartition).getOrElse(0)

				sumLogEndOffset += logEndOffset
				topicPartitionOffsetSet += TopicPartitionLogEndOffset(topic, partition, logEndOffset)
			}

			topicsAndLogEndOffsets += TopicLogEndOffsetInfo(topic, sumLogEndOffset, topicPartitionOffsetSet.toSeq.sorted)
		}

		topicsAndLogEndOffsets.toSeq.sorted
	}

	override def getClusterViz: Node = {
		val clusterNodes = activeTopicPartitionsMap.get().values.map(partition => {
			Node(partition.get(0).leader().host() + ":" + partition.get(0).leader().port(), Seq())
		}).toSet.toSeq.sortWith(_.name < _.name)
		Node("KafkaCluster", clusterNodes)
	}

	def isGroupActive(group: String): Boolean = getActiveGroups.exists(_.equals(group))

	override def getConsumerGroupStatus: String = {
		kafkaOffsetStorage.getConsumerGroups()
	}
}

object KafkaOffsetGetter extends Logging {



	val committedOffsetMap: concurrent.Map[GroupTopicPartition, OffsetAndMetadata] = concurrent.TrieMap()
	val logEndOffsetsMap: concurrent.Map[TopicPartition, Long] = concurrent.TrieMap()
	val kafkaOffsetStorage: KafkaOffsetStorage = new KafkaOffsetStorage();

	// Swap the object on update
	var activeTopicPartitions: AtomicReference[immutable.Set[TopicAndPartition]] = new AtomicReference(immutable.HashSet())
	var clients: AtomicReference[immutable.Set[ClientGroup]] = new AtomicReference(immutable.HashSet())
	var activeTopicAndGroups: AtomicReference[immutable.Set[TopicAndGroup]] = new AtomicReference(immutable.HashSet())
	var activeTopicPartitionsMap: AtomicReference[immutable.Map[String, util.List[PartitionInfo]]] = new AtomicReference(immutable.HashMap())


	private def createNewKafkaConsumer(args: OffsetGetterArgs, group: String, autoCommitOffset: Boolean): KafkaConsumer[Array[Byte], Array[Byte]] = {

		val props: Properties = new Properties

		props.load(new FileInputStream(args.consumerConfig))

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


		props.load(new FileInputStream(args.consumerConfig))


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
									offsetConsumer.seekToBeginning(partitions)
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

								val groupId: String = groupOverview.groupId

								val consumerGroupSummary: Option[List[AdminClient#ConsumerSummary]] = adminClient.describeConsumerGroup(groupId).consumers

								consumerGroupSummary.get.foreach((consumerSummary) => {

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

							activeTopicPartitions.set(newActiveTopicPartitions.toSet)
							clients.set(newClients.toSet)
							activeTopicAndGroups.set(newTopicAndGroups.toSet)
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

		val sleepOnDataRetrieval: Int = 10000
		val sleepOnError: Int = 30000
		var logEndOffsetGetter: KafkaConsumer[Array[Byte], Array[Byte]] = null

		while (true) {

			try {

				while (null == logEndOffsetGetter) {

					val group: String = "kafka-monitor-LogEndOffsetGetter"
					logEndOffsetGetter = createNewKafkaConsumer(args, group, false)
				}

				// Get topic-partitions
				activeTopicPartitionsMap.set(JavaConversions.mapAsScalaMap(logEndOffsetGetter.listTopics()).toMap)
				val distinctPartitionInfo: Seq[PartitionInfo] = (activeTopicPartitionsMap.get().values).flatten(listPartitionInfo => JavaConversions.asScalaBuffer(listPartitionInfo)).toSeq

				// Iterate over each distinct PartitionInfo
				distinctPartitionInfo.foreach(partitionInfo => {

					// Get the LogEndOffset for the TopicPartition
					val topicPartition: TopicPartition = new TopicPartition(partitionInfo.topic, partitionInfo.partition)

					logEndOffsetGetter.assign(Arrays.asList(topicPartition))

					logEndOffsetGetter.seekToEnd(ArrayBuffer(topicPartition).asJava)
					val logEndOffset: Long = logEndOffsetGetter.position(topicPartition)

					// Update KafkaOffsetStorage
					kafkaOffsetStorage.addLogEndOffset(
						new KafkaTopicPartitionLogEndOffset(
							new KafkaTopicPartition(partitionInfo.topic, partitionInfo.partition),
							new KafkaOffsetMetadata(logEndOffset, System.currentTimeMillis())));

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

					Thread.sleep(sleepOnError)
				}
			}
		}
	}
}

case class TopicAndGroup(topic: String, group: String)

case class ClientGroup(group: String, clientId: String, clientHost: String, topicPartitions: Set[TopicPartition])

case class TopicPartitionLogEndOffset(topic: String, partition: Int, logEndOffset: Long) extends Ordered [TopicPartitionLogEndOffset] {

	def compare (that: TopicPartitionLogEndOffset) = {
		if (this.topic == that.topic) {
			if (this.partition > that.partition)
				1
			else if (this.partition < that.partition)
				-1
			else
				0
		}
		else if (this.topic > that.topic)
			1
		else
			-1
	}
}

case class TopicLogEndOffsetInfo(topic: String, sumLogEndOffset: Long, partitions: Seq[TopicPartitionLogEndOffset]) extends Ordered[TopicLogEndOffsetInfo] {

	def compare (that:TopicLogEndOffsetInfo) = {
		if (this.topic == that.topic) 0
		else if (this.topic > that.topic) 1
		else -1
	}
}

//case class OffsetMetadataAndLag
