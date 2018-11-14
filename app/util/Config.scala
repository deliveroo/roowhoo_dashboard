package util

import play.api.Configuration

case class Config(password: String, userName: String, bootstrapServer: String)

case class ZookeeperConfig(port: Int, host: String)

object Config {
  def apply(playConfig: Configuration): Config = {
    val password = playConfig.get[String]("kafka.password")
    val userName = playConfig.get[String]("kafka.userName")
    val bootstrapServer = playConfig.get[String]("bootstrap.server")
    Config(password, userName, bootstrapServer)
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