package models.serializer

import java.util

import models.ConsumerOffsetDetails
import org.apache.kafka.common.errors.SerializationException
import org.apache.kafka.common.serialization.{Deserializer, Serializer}
import play.api.libs.json.Json

object ConsumerOffsetDetailsSerDer extends Serializer[ConsumerOffsetDetails] with  Deserializer[ConsumerOffsetDetails]{

  override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = {}

  override def serialize(topic: String, data: ConsumerOffsetDetails): Array[Byte] = {
    val jsValue = Json.toJson(data)
    Json.toBytes(jsValue)
  }

  override def close(): Unit = {
  }


  override def deserialize(topic: String, data: Array[Byte]): ConsumerOffsetDetails =
    Json.parse(data).validate[ConsumerOffsetDetails].getOrElse(throw new SerializationException)

}
