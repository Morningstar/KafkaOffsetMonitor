package com.quantifind.kafka.core

import com.quantifind.kafka.offsetapp.OffsetGetterArgs
import com.quantifind.utils.ZkUtilsWrapper
import java.nio.{BufferUnderflowException, ByteBuffer}

import kafka.api.{OffsetRequest, OffsetResponse, PartitionOffsetsResponse}
import kafka.common.{OffsetAndMetadata, OffsetMetadata, TopicAndPartition}
import kafka.coordinator._
import kafka.consumer.SimpleConsumer
import kafka.coordinator.group.GroupTopicPartition
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.mockito.Matchers._
import org.mockito.Mockito._
import org.mockito.{Mockito, Matchers => MockitoMatchers}
import org.scalatest._


class KafkaOffsetGetterSpec extends FlatSpec with ShouldMatchers {

  trait Fixture {

		val mockedZkUtil: ZkUtilsWrapper =  Mockito.mock(classOf[ZkUtilsWrapper])
		val mockedConsumer: SimpleConsumer = Mockito.mock(classOf[SimpleConsumer])
    val testPartitionLeader = 1

    val args = new OffsetGetterArgs

		val offsetGetter: KafkaOffsetGetter = new KafkaOffsetGetter(mockedZkUtil, args)
    offsetGetter.consumerMap += (testPartitionLeader -> Some(mockedConsumer))
		val offsetGetterSpy: KafkaOffsetGetter = spy(offsetGetter)

  }

  "KafkaOffsetGetter" should "be able to build offset data for given partition" in new Fixture {

    val testGroup = "testgroup"
    val testTopic = "testtopic"
    val testPartition = 1
    val committedOffset = 100
    val logEndOffset = 102

    val topicAndPartition = TopicAndPartition(testTopic, testPartition)
    val topicPartition = new TopicPartition(testTopic, testPartition)
    val groupTopicPartition = GroupTopicPartition(testGroup, topicPartition)
    val offsetAndMetadata = OffsetAndMetadata(committedOffset, "meta", System.currentTimeMillis)

    KafkaOffsetGetter.committedOffsetMap += (groupTopicPartition -> offsetAndMetadata)
    KafkaOffsetGetter.logEndOffsetsMap += (topicPartition -> logEndOffset)

    when(mockedZkUtil.getLeaderForPartition(MockitoMatchers.eq(testTopic), MockitoMatchers.eq(testPartition)))
        .thenReturn(Some(testPartitionLeader))

    val partitionErrorAndOffsets = Map(topicAndPartition -> PartitionOffsetsResponse(org.apache.kafka.common.protocol.Errors.ILLEGAL_SASL_STATE, Seq(logEndOffset)))
    val offsetResponse = OffsetResponse(1, partitionErrorAndOffsets)
    when(mockedConsumer.getOffsetsBefore(any[OffsetRequest])).thenReturn(offsetResponse)
		when(offsetGetterSpy.isGroupActive(any[String])).thenReturn(true)

		offsetGetterSpy.processPartition(testGroup, testTopic, testPartition) match {
      case Some(offsetInfo) =>
        offsetInfo.topic shouldBe testTopic
        offsetInfo.group shouldBe testGroup
        offsetInfo.partition shouldBe testPartition
        offsetInfo.offset shouldBe committedOffset
        offsetInfo.logSize shouldBe logEndOffset
      case None => fail("Failed to build offset data")
    }
  }

  "tryParseOffsetMessage" should "return None for messages with null keys" in {

    val topic: String = "topic-test"
    val partition: Int = 0
    val offset: Long = 1
    val group: String = "group-test"
    val key: Array[Byte] = null
    val value: Array[Byte] = null

    val cRecord: ConsumerRecord[Array[Byte], Array[Byte]] = new ConsumerRecord[Array[Byte], Array[Byte]](topic, partition, offset, key, value)

    KafkaOffsetGetter.tryParseOffsetMessage(cRecord) shouldBe None
  }

  "tryParseOffsetMessage" should "return None for messages with malformed keys" in {

    val topic: String = "topic-test"
    val partition: Int = 0
    val offset: Long = 1
    val group: String = "group-test"
    val keySmall: Array[Byte] = Array[Byte](1)
    val keyBig: Array[Byte] = BigInt(Long.MaxValue).toByteArray
    val value: Array[Byte] = Array[Byte](1)

    val cRecordSmall: ConsumerRecord[Array[Byte], Array[Byte]] = new ConsumerRecord[Array[Byte], Array[Byte]](topic, partition, offset, keySmall, value)
    KafkaOffsetGetter.tryParseOffsetMessage(cRecordSmall) shouldBe None

    val cRecordBig: ConsumerRecord[Array[Byte], Array[Byte]] = new ConsumerRecord[Array[Byte], Array[Byte]](topic, partition, offset, keyBig, value)
    KafkaOffsetGetter.tryParseOffsetMessage(cRecordBig) shouldBe None
  }

  "tryParseOffsetMessage" should "return None for messages with any key other than OffsetKey" in {

    val topic: String = "topic-test"
    val partition: Int = 0
    val offset: Long = 1
    val group: String = "group-test"
    val key: Array[Byte] = KafkaMessageProtocolHelper.groupMetadataKey(group)
    val value: Array[Byte] = Array[Byte](1)
    val cRecord: ConsumerRecord[Array[Byte], Array[Byte]] = new ConsumerRecord[Array[Byte], Array[Byte]](topic, partition, offset, key, value)

    KafkaOffsetGetter.tryParseOffsetMessage(cRecord) shouldBe None
  }

  "tryParseOffsetMessage" should "return None for messages with a valid OffsetKey and a null body/value" in {

    val topic: String = "topic-test"
    val partition: Int = 0
    val offset: Long = 1
    val group: String = "group-test"

    val key: Array[Byte] = KafkaMessageProtocolHelper.offsetCommitKey(group, topic, partition, 1.toShort)
    val value: Array[Byte] = null
    val cRecord: ConsumerRecord[Array[Byte], Array[Byte]] = new ConsumerRecord[Array[Byte], Array[Byte]](topic, partition, offset, key, value)

    KafkaOffsetGetter.tryParseOffsetMessage(cRecord) shouldBe None
  }

  "tryParseOffsetMessage" should "return None for messages with a valid OffsetKey and a malformed body/value" in {

    val topic: String = "topic-test"
    val partition: Int = 0
    val offset: Long = 1
    val group: String = "group-test"

    val key: Array[Byte] = KafkaMessageProtocolHelper.offsetCommitKey(group, topic, partition, 1.toShort)
    val value: Array[Byte] = Array[Byte](1)
    val cRecord: ConsumerRecord[Array[Byte], Array[Byte]] = new ConsumerRecord[Array[Byte], Array[Byte]](topic, partition, offset, key, value)

    KafkaOffsetGetter.tryParseOffsetMessage(cRecord) shouldBe None
  }

  "tryParseOffsetMessage" should "parse and return valid data for messages with a valid key of type OffsetKey and a valid body/value of type OffsetAndMetadata " in {

    val topic: String = "topic-test"
    val partition: Int = 0
    val offset: Long = 1
    val metadata: String = "metadata-test"
    val group: String = "group-test"
    val commitTimestamp: Long = 12345
    val offsetMetadata: OffsetMetadata = new OffsetMetadata(offset, metadata)
    val offsetAndMetadata: OffsetAndMetadata = new OffsetAndMetadata(offsetMetadata, commitTimestamp, commitTimestamp)

    val key: Array[Byte] = KafkaMessageProtocolHelper.offsetCommitKey(group, topic, partition, 1.toShort)
    val value: Array[Byte] = KafkaMessageProtocolHelper.offsetCommitValue(offsetAndMetadata)
    val cRecord: ConsumerRecord[Array[Byte], Array[Byte]] = new ConsumerRecord[Array[Byte], Array[Byte]](topic, partition, offset, key, value)

    val offsetMessage: Option[(GroupTopicPartition, OffsetAndMetadata)] = KafkaOffsetGetter.tryParseOffsetMessage(cRecord)
    offsetMessage.isDefined shouldBe true

    val messageOffsetMap: (GroupTopicPartition, OffsetAndMetadata) = offsetMessage.get
    val gtp: GroupTopicPartition = messageOffsetMap._1
    val offMeta: OffsetAndMetadata = messageOffsetMap._2
    gtp.group shouldBe group
    gtp.topicPartition shouldBe new TopicPartition(topic, partition)
    offMeta shouldBe offsetAndMetadata
  }
}
