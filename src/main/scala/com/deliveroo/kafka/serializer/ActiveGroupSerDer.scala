package com.deliveroo.kafka.serializer

import java.util

import kafka.coordinator.group.{ActiveGroup}
import org.apache.kafka.common.errors.SerializationException
import org.apache.kafka.common.serialization.{Deserializer, Serializer}
import play.api.libs.json.Json

object ActiveGroupSerDer extends Serializer[ActiveGroup] with  Deserializer[ActiveGroup]{

  override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = {}

  override def serialize(topic: String, data: ActiveGroup): Array[Byte] = {
    val jsValue = Json.toJson(data)
    Json.toBytes(jsValue)
  }

  override def close(): Unit = {
  }


  override def deserialize(topic: String, data: Array[Byte]): ActiveGroup =
    Json.parse(data).validate[ActiveGroup].getOrElse(throw new SerializationException)
}