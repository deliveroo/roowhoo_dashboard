package util

import java.util.Properties

import kafka.coordinator.serializer.ClientDetailsSerde
import org.apache.kafka.common.config.SaslConfigs.SASL_JAAS_CONFIG
import org.apache.kafka.common.config.{SaslConfigs, TopicConfig}
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.StreamsConfig
import play.api.Configuration

case class StreamConfig(password: String, userName: String, bootstrapServer: String, brokerProtocol: String, postFix: String)
case class ZookeeperConfig(port: Int, host: String)

object StreamConfig {
  def OFFSETS_AND_META_WINDOW_STORE_NAME(config: StreamConfig):String = s"active-groups-${config.postFix}"

  def apply(playConfig: Configuration): StreamConfig = {
    val password = playConfig.get[String]("kafka.password")
    val userName = playConfig.get[String]("kafka.userName")
    val bootstrapServer = playConfig.get[String]("bootstrap.server")
    val brokerProtocol = playConfig.get[String]("brokerProtocol")
    val appVersion = playConfig.get[String]("app.version")

    val envType = playConfig.get[String]("env") match {
      case e if e == "dev" || e == "test" => "-dev-"
      case _ => ""
    }

    StreamConfig(password, userName, bootstrapServer, brokerProtocol, s"$envType$appVersion")
  }

  def streamProperties(config: StreamConfig): Properties = {
    val props = new Properties()
    val APP_NAME = s"roowhoo-${config.postFix}"
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, APP_NAME)
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, config.bootstrapServer)
    props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.ByteArray().getClass)
    props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, new ClientDetailsSerde().getClass)
    props.put("exclude.internal.topics", "false") // necessary to consume __consumer_offsets
    props.put(StreamsConfig.CLIENT_ID_CONFIG, APP_NAME)
    val STORE_CHANGE_LOG_ADDITIONAL_RETENTION = 5 * 24 * 60 * 60 * 1000
    props.put(StreamsConfig.WINDOW_STORE_CHANGE_LOG_ADDITIONAL_RETENTION_MS_CONFIG, STORE_CHANGE_LOG_ADDITIONAL_RETENTION.toString)
    props.put(StreamsConfig.TOPIC_PREFIX + TopicConfig.RETENTION_BYTES_CONFIG, "3600000")
    props.put(StreamsConfig.TOPIC_PREFIX + TopicConfig.RETENTION_MS_CONFIG, "3600000")
    props.put(StreamsConfig.TOPIC_PREFIX + TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_DELETE)
    props.put(StreamsConfig.SECURITY_PROTOCOL_CONFIG, s"${config.brokerProtocol}")
    props.put(SaslConfigs.SASL_MECHANISM, "SCRAM-SHA-256")
    props.put(SASL_JAAS_CONFIG, "org.apache.kafka.common.security.scram.ScramLoginModule required " +
      s"""username="${config.userName}"  password="${config.password}";""")
    props.put(StreamsConfig.APPLICATION_SERVER_CONFIG,  "0.0.0.0:9000")
    props.put(StreamsConfig.RETRIES_CONFIG, "5")
    props.put(StreamsConfig.RETRY_BACKOFF_MS_CONFIG, "300")
    props
  }

}

object ZookeeperConfig {
  def apply(playConfig: Configuration): ZookeeperConfig = {
    ZookeeperConfig(
      port = playConfig.get[Int]("zookeeperPort"),
      host = playConfig.get[String]("zookeeperHost")
    )
  }
}