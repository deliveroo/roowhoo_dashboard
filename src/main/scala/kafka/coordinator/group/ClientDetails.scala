package kafka.coordinator.group

import java.nio.ByteBuffer

import play.api.libs.json.{Json, OFormat}


final case class ConsumerInstanceDetails(
                                   id: String,
                                   host: String,
                                   rebalanceTimeoutMs: Long,
                                   sessionTimeoutMs:Long,
                                   protocolType:Set[String],
                                   protocols:Set[String])
final case class ClientDetails(
                                clientId: String,
                                members: Set[ConsumerInstanceDetails],
                                group: String,
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
            members.map(m=> ConsumerInstanceDetails(id=m.memberId,
              host=m.clientHost,
              rebalanceTimeoutMs = m.rebalanceTimeoutMs,
              sessionTimeoutMs = m.sessionTimeoutMs,
              protocolType = m.protocols,
              protocols = m.protocols))
          }.toSet
        ClientDetails(g._1, memberDetails, consumerGroup, current.generationId)
      case _ => empty()
    }

  }

  def empty(): ClientDetails =
    ClientDetails("", Set.empty[ConsumerInstanceDetails], "", 0)

  def isEmpty(clientDetails: ClientDetails): Boolean = clientDetails.members.size == 0

}