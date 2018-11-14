package services.kafkastreams

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import com.lightbend.kafka.scala.streams.DefaultSerdes._
import com.lightbend.kafka.scala.streams.ImplicitConversions._
import com.lightbend.kafka.scala.streams.{KStreamS, KTableS, StreamsBuilderS}
import com.typesafe.config.ConfigFactory
import kafka.coordinator.group.{ActiveGroup, ClientDetails, ConsumerOffsetDetails}
import kafka.coordinator.serializer.{CustomSerdes}
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.common.utils.Bytes
import org.apache.kafka.streams._
import org.apache.kafka.streams.kstream._
import org.apache.kafka.streams.state.WindowStore
import play.api.Logger
import util.StreamConfig
import ConsumerOffsetsFn._


object ConsumerGroupsProcessor  {
  val offsetTopic = "__consumer_offsets"

  implicit val system: ActorSystem = ActorSystem("OffsetsViewer", ConfigFactory.load())
  implicit val _: ActorMaterializer = ActorMaterializer()

  def stream(config: StreamConfig):KafkaStreams = {
    val builder = new StreamsBuilderS()

    val offsetStream: KStreamS[Array[Byte], Array[Byte]] = builder.stream[Array[Byte], Array[Byte]](offsetTopic)
    val Array(offsetKeyStream, groupMetadataKeyStream) = offsetStream.branch(isOffset,isGroupMetadata)

    val offsetCommitsLastTenMins: KStreamS[String, ConsumerOffsetDetails] = offsetKeyStream
      .filterNot(isTombstone)
      .map[String,ConsumerOffsetDetails](offsetConsumerGroupKey)
      .filter(isCommittedLastTenMins)


    val groupMetadataCommits: KTableS[String, ClientDetails] = groupMetadataKeyStream
      .filterNot(isTombstone)
      .map[String, ClientDetails](groupMetadataConsumerGroupKey)
      .filterNot((_, c) => ClientDetails.isEmpty(c))
      .groupByKey(Serialized.`with`(Serdes.String(), CustomSerdes.clientDetailsSerde))
      .aggregate(newGroupMetadataAggregateInit, newLatestGroupMeta)

    implicit val joinedImp = joinedFromKVOSerde(Serdes.String(), CustomSerdes.consumerOffsetDetailsSerde,  CustomSerdes.clientDetailsSerde)
    val joined: KStreamS[String, ActiveGroup] = offsetCommitsLastTenMins
      .join(
        groupMetadataCommits,
        (offsetCommit:ConsumerOffsetDetails, groupMetadata:ClientDetails) => {
          ActiveGroup(groupMetadata, offsetCommit)
        }
      )
    joined
      .groupByKey(Serialized.`with`(Serdes.String(), CustomSerdes.activeGroup))
      .windowedBy(TimeWindows.of(60000))
      .reduce(
        (a1: ActiveGroup, a2: ActiveGroup) => {
          a2},
        Materialized
          .as[String, ActiveGroup, WindowStore[Bytes, Array[Byte]]](StreamConfig.OFFSETS_AND_META_WINDOW_STORE_NAME(config))
          .withKeySerde(Serdes.String())
          .withValueSerde(CustomSerdes.activeGroup)
      )

    val topology = builder.build()
    val streams: KafkaStreams = new KafkaStreams(topology, StreamConfig.streamProperties(config))
    streams.start()
    Logger.info(topology.describe().toString)
    streams
  }

  def shutdown(stream: KafkaStreams): Unit = {
    stream.close()
    stream.cleanUp()
  }
}
