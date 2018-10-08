package com.deliveroo.kafka.consumer.offsets.viewer

import java.nio.ByteBuffer
import java.time.Instant
import java.util.Properties

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.server.Route
import akka.stream.ActorMaterializer
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.LazyLogging
import kafka.coordinator.group.{GroupMetadataKey, GroupMetadataManager, OffsetKey}
import org.apache.kafka.common.config.SaslConfigs
import org.apache.kafka.common.config.SaslConfigs.SASL_JAAS_CONFIG
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.common.utils.Bytes
import org.apache.kafka.streams._
import org.apache.kafka.streams.kstream._
import org.apache.kafka.streams.state.WindowStore
import org.rogach.scallop.ScallopConf

import scala.concurrent.Await
import scala.concurrent.duration._

class Conf(arguments: Seq[String]) extends ScallopConf(arguments) {
  val broker = opt[String](required = true, default = Some(sys.env.getOrElse("BROKER_BOOTSTRAP", "localhost:9092")))
  verify()
}

object OffsetsViewer extends LazyLogging  {
  val offsetTopic:String = sys.env.get("INPUT_TOPIC").get

  val OFFSETS_STORE_NAME = "offsets-state-store"
  val GROUP_TOPIC_MAX_PARTITION_STORE_NAME = "group-topic-maxpartition"
  val OFFSETS_AND_META_WINDOW_STORE_NAME = "active-groups"

  def streamProperties(broker:String) = {
    val props = new Properties()
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "roowhoo")
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, broker)
    props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.ByteArray().getClass)
    props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.ByteArray.getClass)
    props.put("exclude.internal.topics", "false") // necessary to consume __consumer_offsets

    props.put(StreamsConfig.SECURITY_PROTOCOL_CONFIG, "SASL_SSL")
    props.put(SaslConfigs.SASL_MECHANISM, "SCRAM-SHA-256")
    props.put(SASL_JAAS_CONFIG, "org.apache.kafka.common.security.scram.ScramLoginModule required " +
      s"""username="<set-me>"  password="<set-me>";""")

    props.put(StreamsConfig.RETRIES_CONFIG, "5")
    props.put(StreamsConfig.RETRY_BACKOFF_MS_CONFIG, "300")
    props
  }

  implicit val system: ActorSystem = ActorSystem("OffsetsViewer", ConfigFactory.load())
  implicit val _: ActorMaterializer = ActorMaterializer()

  private val isOffset: Predicate[Array[Byte], Array[Byte]] = (key, _) => {
    GroupMetadataManager.readMessageKey(ByteBuffer.wrap(key)) match {
      case _: OffsetKey => true
      case _: GroupMetadataKey => false
    }
  }

  private val isGroupMetadata: Predicate[Array[Byte], Array[Byte]] = (key, value) => {
    !isOffset.test(key, value)
  }

  private val isCommittedLastTenMins: Predicate[String, String] = (_,v) => {
    logger.info(s"####v: ${v}")
    val consumerGroupData = v.split("\\|")
    if (consumerGroupData.length <= 2) {
      false
    } else {
      val commitTs = Instant.ofEpochMilli(consumerGroupData(2).toLong)
      val now = Instant.now()

      val tenMinutesAgo = now.minusSeconds(180)

      if(commitTs.compareTo(tenMinutesAgo) < 1) false else true
    }
  }

  private val offsetConsumerGroupKey: KeyValueMapper[Array[Byte], Array[Byte], KeyValue[String,String]] = (k: Array[Byte], v: Array[Byte]) => {
    val offsetKey = GroupMetadataManager.readMessageKey(ByteBuffer.wrap(k)).asInstanceOf[OffsetKey]

    val (commitTimestamp, expireTimestamp) = Option(v).map { case (offsetValue) =>
      val messageValue = GroupMetadataManager.readOffsetMessageValue(ByteBuffer.wrap(offsetValue))
      (messageValue.commitTimestamp.toString, messageValue.expireTimestamp.toString)
    } getOrElse{
      logger.info(s"###empty val ${v} for key: ${offsetKey}")
      ("","")
    }

    KeyValue.pair(offsetKey.key.group, s"${offsetKey.key.topicPartition.topic()}|${offsetKey.key.topicPartition.partition()}|${commitTimestamp}|${expireTimestamp}")
  }

  private val groupMetadataConsumerGroupKey: KeyValueMapper[Array[Byte], Array[Byte], KeyValue[String, Array[Byte]]] = (k: Array[Byte], v: Array[Byte]) => {
    KeyValue.pair(
      GroupMetadataManager.readMessageKey(ByteBuffer.wrap(k)).asInstanceOf[GroupMetadataKey].toString, v)
  }

  private val groupMetadataAggregateInit: Initializer[Array[Byte]] = () => Array[Byte]()
  private val latestGroupMeta: Aggregator[String, Array[Byte], Array[Byte]] = (consumerGroupName:String, v: Array[Byte], agg:Array[Byte]) => {
    if(agg.length > 0) {
      val currentLatest = GroupMetadataManager.readGroupMessageValue(consumerGroupName, ByteBuffer.wrap(agg))
      val current = GroupMetadataManager.readGroupMessageValue(consumerGroupName, ByteBuffer.wrap(v))
      if (current.generationId > currentLatest.generationId) v else agg
    } else {
      v
    }
  }

  private val offsetCommitToMetadataValueJoiner: ValueJoiner[String,Array[Byte],String] = (offsetCommit:String, groupMetadata:Array[Byte]) => {
    val offsetCommitFields = offsetCommit.split("\\|")
    val consumerGroup = offsetCommitFields(0)
    s"${offsetCommit}|${GroupMetadataManager.readGroupMessageValue(consumerGroup, ByteBuffer.wrap(groupMetadata)).toString}"
  }

  private val offsetCommitToMetaValueReducer: Reducer[String] = (_:String, v2:String) => {
    v2
  }

  def main(args: Array[String]):Unit = {
    val conf = new Conf(args)
    val builder = new StreamsBuilder

    val offsetStream = builder.stream[Array[Byte], Array[Byte]](offsetTopic)
    val Array(offsetKeyStream, groupMetadataKeyStream) = offsetStream.branch(isOffset,isGroupMetadata)

    val offsetCommitsLastTenMins: KStream[String, String] = offsetKeyStream
      .map[String,String](offsetConsumerGroupKey)
      .filter(isCommittedLastTenMins)

    val groupMetadataCommits: KTable[String, Array[Byte]] = groupMetadataKeyStream
      .map[String, Array[Byte]](groupMetadataConsumerGroupKey)
      .groupByKey(Serialized.`with`(Serdes.String(), Serdes.ByteArray()))
      .aggregate(groupMetadataAggregateInit, latestGroupMeta)

    val joined: KStream[String, String] = offsetCommitsLastTenMins
      .join(
        groupMetadataCommits,
        offsetCommitToMetadataValueJoiner,
        Joined.`with`(Serdes.String(), Serdes.String(), Serdes.ByteArray())
      )
    joined
      .groupByKey(Serialized.`with`(Serdes.String(), (Serdes.String())))
      .windowedBy(TimeWindows.of(60000))
      .reduce(
        offsetCommitToMetaValueReducer,
        Materialized
          .as[String, String, WindowStore[Bytes, Array[Byte]]](OFFSETS_AND_META_WINDOW_STORE_NAME)
          .withKeySerde(Serdes.String())
          .withValueSerde(Serdes.String())
      )

    val streams: KafkaStreams = new KafkaStreams(builder.build(), streamProperties(conf.broker()))
    streams.start()

    val routes: Route = OffsetsEndpoints.routesFor(streams)
    val serverBinding: Http.ServerBinding = Await.result(Http().bindAndHandle(routes, "0.0.0.0", 8082), 60.seconds)

    sys.addShutdownHook{
      serverBinding.unbind()
      streams.close()
    }

  }

}
