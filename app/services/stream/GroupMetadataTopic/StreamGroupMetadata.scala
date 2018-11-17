package services.stream.GroupMetadataTopic

import com.lightbend.kafka.scala.streams.DefaultSerdes._
import com.lightbend.kafka.scala.streams.ImplicitConversions._
import com.lightbend.kafka.scala.streams.{KStreamS, KTableS, StreamsBuilderS}
import models.serializer.CustomSerdes
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.common.utils.Bytes
import org.apache.kafka.streams._
import org.apache.kafka.streams.kstream._
import org.apache.kafka.streams.state.WindowStore
import play.api.Logger
import util.StreamConfig
import GroupMetadataHelper._
import models._
import org.apache.kafka.common.internals.Topic


object StreamGroupMetadata  {
  val GROUP_METADATA_TOPIC_NAME: TopicName = Topic.GROUP_METADATA_TOPIC_NAME //__consumer_offsets

  def stream(config: StreamConfig):KafkaStreams = {
    val builder = new StreamsBuilderS()

    val offsetStream: KStreamS[Array[Byte], Array[Byte]] = builder.stream[Array[Byte], Array[Byte]](GROUP_METADATA_TOPIC_NAME)
    val Array(offsetKeyStream, groupMetadataKeyStream) = offsetStream
      .filterNot(isTombstone)
      .branch(isOffset,isGroupMetadata)

    val offsetCommitsLastTenMins: KStreamS[GroupId, ConsumerOffsetDetails] = offsetKeyStream
      .map[GroupId,ConsumerOffsetDetails](offsetConsumerGroupKey)
      .filter(isCommittedLastTenMins)


    val groupMetadataCommits: KTableS[GroupId, ClientDetails] = groupMetadataKeyStream
      .map[GroupId, ClientDetails](groupMetadataConsumerGroupKey)
      .filterNot((_, c) => ClientDetails.isEmpty(c))
      .groupByKey(Serialized.`with`(Serdes.String(), CustomSerdes.clientDetailsSerde))
      .aggregate(newGroupMetadataAggregateInit, newLatestGroupMeta)

    implicit val joinedImp = joinedFromKVOSerde(Serdes.String(), CustomSerdes.consumerOffsetDetailsSerde,  CustomSerdes.clientDetailsSerde)
    val joined: KStreamS[GroupId, ActiveGroup] = offsetCommitsLastTenMins
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
          .as[GroupId, ActiveGroup, WindowStore[Bytes, Array[Byte]]](StreamConfig.OFFSETS_AND_META_WINDOW_STORE_NAME(config))
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
