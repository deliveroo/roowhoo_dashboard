package kafka.coordinator.group

import java.nio.ByteBuffer

import org.apache.kafka.clients.consumer.internals.ConsumerProtocol
import org.apache.kafka.common.TopicPartition
import play.api.libs.json.{Json, OFormat}

import scala.collection.JavaConverters._

object ALIAS {
  type Topic= String
  type PartitionNumber = Int
  type ConsumerInstanceId = String
  type GroupId = String
  type ClientId = String
}
final case class ConsumerInstanceDetails(
                                   id: ALIAS.ConsumerInstanceId,
                                   host: String,
                                   rebalanceTimeoutMs: Long,
                                   sessionTimeoutMs:Long,
                                   protocolType:Set[String],
                                   protocols:Set[String],
                                   assignedPartitions: Set[(ALIAS.Topic, ALIAS.PartitionNumber)])
final case class ClientDetails(
                                clientId: ALIAS.ClientId,
                                members: Set[ConsumerInstanceDetails],
                                group: ALIAS.GroupId,
                                generationId: Long
                                )

object ClientDetails {

  implicit val consumerInstanceDetailsJson: OFormat[ConsumerInstanceDetails] = Json.format[ConsumerInstanceDetails]
  implicit val clientDetailsJson: OFormat[ClientDetails] = Json.format[ClientDetails]

  def apply(consumerGroup: String, value: Array[Byte]): ClientDetails = {

    val current = GroupMetadataManager.readGroupMessageValue(consumerGroup,ByteBuffer.wrap(value))
    val grouped = current.allMemberMetadata.groupBy(m => m.clientId)
    val clientId = grouped.headOption
    clientId match {
      case Some(g) =>
        val memberDetails = grouped
          .flatten{case (_, members) =>
            members.map(m=> {
              val assignedPartitions = ConsumerProtocol.deserializeAssignment(ByteBuffer.wrap(m.assignment))
                .partitions().asScala.toSet.map((p:TopicPartition) => (p.topic(), p.partition()))
              ConsumerInstanceDetails(id=m.memberId,
                host=m.clientHost,
                rebalanceTimeoutMs = m.rebalanceTimeoutMs,
                sessionTimeoutMs = m.sessionTimeoutMs,
                protocolType = m.protocols,
                protocols = m.protocols,
                assignedPartitions = assignedPartitions)
            })
          }.toSet
        ClientDetails(g._1, memberDetails, consumerGroup, current.generationId)
      case _ => empty()
    }

  }

  def empty(): ClientDetails =
    ClientDetails("", Set.empty[ConsumerInstanceDetails], "", 0)

  def isEmpty(clientDetails: ClientDetails): Boolean = clientDetails.members.size == 0

}