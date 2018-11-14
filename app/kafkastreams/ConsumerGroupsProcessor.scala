package kafkastreams

import java.util.Properties

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import com.lightbend.kafka.scala.streams.DefaultSerdes._
import com.lightbend.kafka.scala.streams.ImplicitConversions._
import com.lightbend.kafka.scala.streams.{KStreamS, KTableS, StreamsBuilderS}
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.LazyLogging
import kafka.coordinator.group.{ActiveGroup, ClientDetails, ConsumerOffsetDetails}
import kafka.coordinator.serializer.{ClientDetailsSerde, CustomSerdes}
import org.apache.kafka.common.config.SaslConfigs
import org.apache.kafka.common.config.SaslConfigs.SASL_JAAS_CONFIG
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.common.utils.Bytes
import org.apache.kafka.streams._
import org.apache.kafka.streams.kstream._
import org.apache.kafka.streams.state.WindowStore
import util.Config


object ConsumerGroupsProcessor extends LazyLogging  {
  import ConsumerOffsetsFn._

  val offsetTopic:String = "__consumer_offsets"

  val OFFSETS_AND_META_WINDOW_STORE_NAME = "active-groups"

  def streamProperties(config: Config) = {
    val props = new Properties()
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "roowhoo")
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, config.bootstrapServer)
    props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.ByteArray().getClass)
    props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, new ClientDetailsSerde().getClass)
    props.put("exclude.internal.topics", "false") // necessary to consume __consumer_offsets

    props.put(StreamsConfig.SECURITY_PROTOCOL_CONFIG, "SASL_SSL")
    props.put(SaslConfigs.SASL_MECHANISM, "SCRAM-SHA-256")
    props.put(SASL_JAAS_CONFIG, "org.apache.kafka.common.security.scram.ScramLoginModule required " +
      s"""username="${config.userName}"  password="${config.password}";""")
    props.put(StreamsConfig.APPLICATION_SERVER_CONFIG,  "0.0.0.0:8082")
    props.put(StreamsConfig.RETRIES_CONFIG, "5")
    props.put(StreamsConfig.RETRY_BACKOFF_MS_CONFIG, "300")
    props
  }

  implicit val system: ActorSystem = ActorSystem("OffsetsViewer", ConfigFactory.load())
  implicit val _: ActorMaterializer = ActorMaterializer()

  def stream(config: Config):KafkaStreams = {
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
          .as[String, ActiveGroup, WindowStore[Bytes, Array[Byte]]](OFFSETS_AND_META_WINDOW_STORE_NAME)
          .withKeySerde(Serdes.String())
          .withValueSerde(CustomSerdes.activeGroup)
      )

    val streams: KafkaStreams = new KafkaStreams(builder.build(), streamProperties(config))
    streams.start()
    streams
    //    val routes: Route = ConsumerGroupsEndpoints.routesFor(streams)
    //    val serverBinding: Http.ServerBinding = Await.result(Http().bindAndHandle(routes, "0.0.0.0", 8082), 60.seconds)
    //
    //    sys.addShutdownHook{
    //      serverBinding.unbind()
    //      streams.close()
    //    }

  }

  def shutdown(stream: KafkaStreams): Unit = {
    stream.close()
  }
}
