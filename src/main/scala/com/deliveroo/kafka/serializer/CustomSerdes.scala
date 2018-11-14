package com.deliveroo.kafka.serializer

import com.lightbend.kafka.scala.streams.StatelessScalaSerde
import kafka.coordinator.group.{ActiveGroup, ClientDetails, ConsumerOffsetDetails}
import org.apache.kafka.common.serialization.{Serde, Serdes}
import play.api.libs.json.Json

object CustomSerdes {
  val clientDetailsSerde: Serde[ClientDetails] = Serdes.serdeFrom(ClientDetailsSerDer, ClientDetailsSerDer)
  val consumerOffsetDetailsSerde: Serde[ConsumerOffsetDetails] = Serdes.serdeFrom(ConsumerOffsetDetailsSerDer, ConsumerOffsetDetailsSerDer)
  val activeGroup: Serde[ActiveGroup] = Serdes.serdeFrom(ActiveGroupSerDer, ActiveGroupSerDer)
}

class ClientDetailsSerde extends StatelessScalaSerde[ClientDetails] {
  override def serialize(data: ClientDetails): Array[Byte] = {
    val jsValue = Json.toJson(data)
    Json.toBytes(jsValue)
  }

  override def deserialize(data: Array[Byte]): Option[ClientDetails] = Json.parse(data).validate[ClientDetails].asOpt
}