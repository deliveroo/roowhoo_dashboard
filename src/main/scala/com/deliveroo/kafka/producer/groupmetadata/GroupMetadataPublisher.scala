package com.deliveroo.kafka.producer.groupmetadata

import java.util.Properties

import com.typesafe.scalalogging.LazyLogging
import kafka.common.OffsetAndMetadata
import kafka.coordinator.group.GroupMetadataRecordWrapper._
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.producer._
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.config.SaslConfigs
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.rogach.scallop.ScallopConf

object GroupMetadataPublisher extends LazyLogging{

  private val props = new Properties() {
    put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[ByteArraySerializer].getName)
    put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[ByteArraySerializer].getName)
    put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, sys.env.getOrElse("BROKER_BOOTSTRAP", "localhost:6001"))

    put("exclude.internal.topics", "false")

    put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_SSL")
    put(SaslConfigs.SASL_MECHANISM, "SCRAM-SHA-256")
    put(SaslConfigs.SASL_JAAS_CONFIG, "org.apache.kafka.common.security.scram.ScramLoginModule required " +
      s"""username="<username>"  password="<password>";""")
  }
  val offsetTopic:String = sys.env.get("INPUT_TOPIC").get

  private val producer = new KafkaProducer[Array[Byte], Array[Byte]](props)

  case class PartitionOffset(partition:Int, offset:Long)
  case class ConsumerGroup(groupName:String, topic:String, partitionOffsets:Seq[PartitionOffset])

  class Conf(arguments: Seq[String]) extends ScallopConf(arguments) {
    val groupToPurge = opt[String](required = false)
    verify()
  }

  def main(args: Array[String]): Unit = {
    val conf = new Conf(args)
    val groupToPurge = conf.groupToPurge.toOption

    val consumerGroups = Seq(
      ConsumerGroup(
        "perf-consumer-3031",
        "test-canary",
        Seq(
          PartitionOffset(0, 1L), PartitionOffset(1, 1L), PartitionOffset(2, 1L), PartitionOffset(3, 1L),
          PartitionOffset(4, 3L), PartitionOffset(5, 4L), PartitionOffset(6, 7L), PartitionOffset(7, 9L),
          PartitionOffset(8, 9L), PartitionOffset(9, 4L), PartitionOffset(10, 7L), PartitionOffset(11, 9L),
          PartitionOffset(12, 9L), PartitionOffset(13, 4L), PartitionOffset(14, 7L), PartitionOffset(15, 9L)
        )
      )
//      ConsumerGroup(
//        "consumerGroupOne",
//        "topicOne",
//        Seq(
//          PartitionOffset(0, 1L), PartitionOffset(1, 1L), PartitionOffset(2, 1L), PartitionOffset(3, 1L),
//          PartitionOffset(0, 3L), PartitionOffset(1, 4L), PartitionOffset(2, 7L), PartitionOffset(3, 9L),
//          PartitionOffset(0, 5L), PartitionOffset(1, 6L), PartitionOffset(2, 9L), PartitionOffset(3, 11L)
//        )
//      )
//      ,
//      ConsumerGroup(
//        "consumerGroupTwo",
//        "topicTwo",
//        Seq(
//          PartitionOffset(0, 1L), PartitionOffset(1, 1L), PartitionOffset(2, 1L),
//          PartitionOffset(0, 33L), PartitionOffset(1, 44L), PartitionOffset(2, 77L),
//          PartitionOffset(0, 45L), PartitionOffset(1, 56L), PartitionOffset(2, 79L)
//        )
//      )
    )

    consumerGroups.foreach(consumerGroup => {
      consumerGroup.partitionOffsets.foreach(partitionOffset => {
        val topicPartition = new TopicPartition(consumerGroup.topic, partitionOffset.partition)
        val key: Array[Byte] = offsetCommitRecordKey(consumerGroup.groupName, topicPartition)

        val value: Array[Byte] = groupToPurge match {
          case Some(groupName) => if (isToBePurged(consumerGroup, groupName)) null else offsetCommitRecordValue(OffsetAndMetadata(partitionOffset.offset))
          case None => offsetCommitRecordValue(OffsetAndMetadata(partitionOffset.offset))
        }

        val record = new ProducerRecord[Array[Byte], Array[Byte]](offsetTopic, key, value)
        val metadata: RecordMetadata = producer.send(record).get()
        logger.info(s"recordmetadata: ${metadata}")
        logger.info(s"sent offset ${partitionOffset.offset}/value ${value}, for partition: ${partitionOffset.partition} for topic: ${consumerGroup.topic} for group ${consumerGroup.groupName}")
      })

      groupToPurge.map (g => {
        if (isToBePurged(consumerGroup, g)) {
          val groupMetadata = producer.send(new ProducerRecord[Array[Byte], Array[Byte]](offsetTopic, groupMetadataKey(g), null)).get()
          logger.info(s"groupMetadata: ${groupMetadata}")
        }
      })

    })

  }

  private def isToBePurged(consumerGroup: ConsumerGroup, groupName: String) = {
    consumerGroup.groupName.equals(groupName)
  }
}
