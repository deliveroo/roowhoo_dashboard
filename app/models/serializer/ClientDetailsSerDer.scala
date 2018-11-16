package models.serializer

import java.util

import models.ClientDetails
import org.apache.kafka.common.errors.SerializationException
import org.apache.kafka.common.serialization.{Deserializer, Serializer}
import play.api.libs.json.Json

object ClientDetailsSerDer extends Serializer[ClientDetails] with  Deserializer[ClientDetails]{

  override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = {}

  override def serialize(topic: String, data: ClientDetails): Array[Byte] = {
    val jsValue = Json.toJson(data)
    Json.toBytes(jsValue)
  }

  override def close(): Unit = {
  }


  override def deserialize(topic: String, data: Array[Byte]): ClientDetails =
    Json.parse(data).validate[ClientDetails].getOrElse(throw new SerializationException)
}
