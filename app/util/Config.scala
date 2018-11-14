package util

import play.api.Configuration

case class Config(password: String, userName: String, bootstrapServer: String)

object Config {
  def apply(playConfig: Configuration): Config = {
    val password = playConfig.get[String]("kafka.password")
    val userName = playConfig.get[String]("kafka.userName")
    val bootstrapServer = playConfig.get[String]("bootstrap.server")
    Config(password, userName, bootstrapServer)
  }
}