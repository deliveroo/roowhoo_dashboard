package util

import java.net.InetAddress

import play.api.Configuration

case class Config(password: String, userName: String, bootstrapServer: String, appVersion: String)

case class ZookeeperConfig(port: Int, host: String)

object Config {
  def apply(playConfig: Configuration): Config = {
    val password = playConfig.get[String]("kafka.password")
    val userName = playConfig.get[String]("kafka.userName")
    val bootstrapServer = playConfig.get[String]("bootstrap.server")
    val appVersion = playConfig.get[String]("app.version")
    Config(password, userName, bootstrapServer, appVersion)
  }
}

object ZookeeperConfig {
  def apply(playConfig: Configuration): ZookeeperConfig = {
    ZookeeperConfig(
      port = playConfig.get[Int]("zookeeperPort"),
      host = playConfig.get[String]("zookeeperHost") match {
        case "zookeeper" => InetAddress.getByName("zookeeper").getHostAddress
        case localhost => localhost
      }
    )
  }
}