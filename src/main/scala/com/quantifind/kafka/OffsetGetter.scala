package com.quantifind.kafka

import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicBoolean

import com.quantifind.kafka.OffsetGetter.{BrokerInfo, KafkaInfo, OffsetInfo}
import com.quantifind.kafka.core._
import com.quantifind.kafka.offsetapp.OffsetGetterArgs
import com.quantifind.utils.ZkUtilsWrapper
import com.twitter.util.Time
import kafka.common.BrokerNotAvailableException
import kafka.consumer.{ConsumerConnector, SimpleConsumer}
import kafka.utils.{Json, Logging, ZkUtils}
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.security.JaasUtils

import scala.collection._
import scala.util.control.NonFatal


case class Node(name: String, children: Seq[Node] = Seq())

case class TopicDetails(consumers: Seq[ConsumerDetail])

case class TopicDetailsWrapper(consumers: TopicDetails)

case class TopicAndConsumersDetails(active: Seq[KafkaInfo], inactive: Seq[KafkaInfo])

case class TopicAndConsumersDetailsWrapper(consumers: TopicAndConsumersDetails)

case class ConsumerDetail(name: String)

trait OffsetGetter extends Logging {

	val consumerMap: mutable.Map[Int, Option[SimpleConsumer]] = mutable.Map()

	def zkUtils: ZkUtilsWrapper

	//  kind of interface methods
	def getTopicList(group: String): List[String]

	def getGroups: Seq[String]

	def getTopicMap: Map[String, Seq[String]]

	def getActiveTopicMap: Map[String, Seq[String]]

	def processPartition(group: String, topic: String, pid: Int): Option[OffsetInfo]

	// get the Kafka simple consumer so that we can fetch broker offsets
	protected def getConsumer(bid: Int): Option[SimpleConsumer] = {
		try {
			zkUtils.readDataMaybeNull(ZkUtils.BrokerIdsPath + "/" + bid) match {
				case (Some(brokerInfoString), _) =>
					Json.parseFull(brokerInfoString) match {
						case Some(m) =>
							val brokerInfo = m.asInstanceOf[Map[String, Any]]
							val host = brokerInfo.get("host").get.asInstanceOf[String]
							val port = brokerInfo.get("port").get.asInstanceOf[Int]
							Some(new SimpleConsumer(host, port, 10000, 100000, "ConsumerOffsetChecker"))
						case None =>
							throw new BrokerNotAvailableException("Broker id %d does not exist".format(bid))
					}
				case (None, _) =>
					throw new BrokerNotAvailableException("Broker id %d does not exist".format(bid))
			}
		} catch {
			case t: Throwable =>
				error("Could not parse broker info", t)
				None
		}
	}

	protected def processTopic(group: String, topic: String): Seq[OffsetInfo] = {
		val pidMap = zkUtils.getPartitionsForTopics(Seq(topic))
		for {
			partitions <- pidMap.get(topic).toSeq
			pid <- partitions.sorted
			info <- processPartition(group, topic, pid)
		} yield info
	}

	protected def brokerInfo(): Iterable[BrokerInfo] = {
		for {
			(bid, consumerOpt) <- consumerMap
			consumer <- consumerOpt
		} yield BrokerInfo(id = bid, host = consumer.host, port = consumer.port)
	}

	protected def offsetInfo(group: String, topics: Seq[String] = Seq()): Seq[OffsetInfo] = {

		val topicList = if (topics.isEmpty) {
			getTopicList(group)
		} else {
			topics
		}

		topicList.sorted.flatMap(processTopic(group, _))
	}


	// get information about a consumer group and the topics it consumes
	def getInfo(group: String, topics: Seq[String] = Seq()): KafkaInfo = {
		val off = offsetInfo(group, topics)
		val brok = brokerInfo()
		KafkaInfo(
			name = group,
			brokers = brok.toSeq,
			offsets = off
		)
	}

	// get list of all topics
	def getTopics: Seq[String] = {
		try {
			zkUtils.getChildren(ZkUtils.BrokerTopicsPath).sortWith(_ < _)
		} catch {
			case NonFatal(t) =>
				error(s"could not get topics because of ${t.getMessage}", t)
				Seq()
		}
	}

	def getClusterViz: Node = {
		val clusterNodes = zkUtils.getAllBrokersInCluster().map((broker) => {
			Node(broker.toString(), Seq())
		})
		Node("KafkaCluster", clusterNodes)
	}

	/**
	  * Returns details for a given topic such as the consumers pulling off of it
	  */
	def getTopicDetail(topic: String): TopicDetails = {
		val topicMap = getActiveTopicMap

		if (topicMap.contains(topic)) {
			TopicDetails(topicMap(topic).map(consumer => {
				ConsumerDetail(consumer.toString)
			}).toSeq)
		} else {
			TopicDetails(Seq(ConsumerDetail("Unable to find Active Consumers")))
		}
	}


	def getGroupTopicDetail(group: String, topic: String): Seq[OffsetInfo] = {
		offsetInfo(group, Seq(topic))
	}


	def mapConsumerDetails(consumers: Seq[String]): Seq[ConsumerDetail] =
		consumers.map(consumer => ConsumerDetail(consumer.toString))

	/**
	  * Returns details for a given topic such as the active consumers pulling off of it
	  * and for each of the active consumers it will return the consumer data
	  */
	def getTopicAndConsumersDetail(topic: String): TopicAndConsumersDetailsWrapper = {
		val topicMap = getTopicMap
		val activeTopicMap = getActiveTopicMap

		val activeConsumers = if (activeTopicMap.contains(topic)) {
			mapConsumersToKafkaInfo(activeTopicMap(topic), topic)
		} else {
			Seq()
		}

		val inactiveConsumers = if (!activeTopicMap.contains(topic) && topicMap.contains(topic)) {
			mapConsumersToKafkaInfo(topicMap(topic), topic)
		} else if (activeTopicMap.contains(topic) && topicMap.contains(topic)) {
			mapConsumersToKafkaInfo(topicMap(topic).diff(activeTopicMap(topic)), topic)
		} else {
			Seq()
		}

		TopicAndConsumersDetailsWrapper(TopicAndConsumersDetails(activeConsumers, inactiveConsumers))
	}

	def mapConsumersToKafkaInfo(consumers: Seq[String], topic: String): Seq[KafkaInfo] =
		consumers.map(getInfo(_, Seq(topic)))


	def getActiveTopics: Node = {
		val topicMap = getActiveTopicMap

		Node("ActiveTopics", topicMap.map {
			case (s: String, ss: Seq[String]) => {
				Node(s, ss.map(consumer => Node(consumer)))

			}
		}.toSeq)
	}
}

object OffsetGetter {

	case class KafkaInfo(name: String, brokers: Seq[BrokerInfo], offsets: Seq[OffsetInfo])

	case class BrokerInfo(id: Int, host: String, port: Int)

	case class OffsetInfo(group: String,
	                      topic: String,
	                      partition: Int,
	                      offset: Long,
	                      logSize: Long,
	                      owner: Option[String],
	                      creation: Time,
	                      modified: Time) {
		val lag = logSize - offset
	}

	val kafkaOffsetListenerStarted: AtomicBoolean = new AtomicBoolean(false)
	var zkUtils: ZkUtilsWrapper = null
	var consumerConnector: ConsumerConnector = null
	var newKafkaConsumer: KafkaConsumer[String, String] = null

	def createZkUtils(args: OffsetGetterArgs): ZkUtils = {
		ZkUtils(args.zk,
			args.zkSessionTimeout.toMillis.toInt,
			args.zkConnectionTimeout.toMillis.toInt,
			JaasUtils.isZkSecurityEnabled())
	}

	def getInstance(args: OffsetGetterArgs): OffsetGetter = {

		if (kafkaOffsetListenerStarted.compareAndSet(false, true)) {

			zkUtils = new ZkUtilsWrapper(createZkUtils(args))

			if (args.offsetStorage.toLowerCase == "kafka") {

				val adminClientExecutor = Executors.newSingleThreadExecutor()
				adminClientExecutor.submit(new Runnable() {
					def run() = KafkaOffsetGetter.startAdminClient(args)
				})

				val logEndOffsetExecutor = Executors.newSingleThreadExecutor()
				logEndOffsetExecutor.submit(new Runnable() {
					def run() = KafkaOffsetGetter.startLogEndOffsetGetter(args)
				})

				val committedOffsetExecutor = Executors.newSingleThreadExecutor()
				committedOffsetExecutor.submit(new Runnable() {
					def run() = KafkaOffsetGetter.startCommittedOffsetListener(args)
				})
			}
		}

		args.offsetStorage.toLowerCase match {
			case "kafka" =>
				new KafkaOffsetGetter(zkUtils, args)
			case "storm" =>
				new StormOffsetGetter(zkUtils, args.stormZKOffsetBase)
			case _ =>
				new ZKOffsetGetter(zkUtils)
		}
	}
}