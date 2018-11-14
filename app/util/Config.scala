package util

import play.api.Configuration
case class Config(password: String, userName: String, bootstrapServer: String, brokerProtocol: String, postFix: String)

case class ZookeeperConfig(port: Int, host: String)

object Config {
  def apply(playConfig: Configuration): Config = {
    val password = playConfig.get[String]("kafka.password")
    val userName = playConfig.get[String]("kafka.userName")
    val bootstrapServer = playConfig.get[String]("bootstrap.server")
    val brokerProtocol = playConfig.get[String]("brokerProtocol")
    val appVersion = playConfig.get[String]("app.version")

    val envType = playConfig.get[String]("env") match {
      case e if e == "dev" || e == "test" => "-dev-"
      case _ => ""
    }

    Config(password, userName, bootstrapServer, brokerProtocol, s"$envType$appVersion")
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