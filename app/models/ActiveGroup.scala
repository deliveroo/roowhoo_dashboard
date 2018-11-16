package models

import play.api.libs.json.{Json, OFormat}

final case class ActiveGroup(clientDetails: ClientDetails, consumerOffsets: ConsumerOffsetDetails)
object ActiveGroup {
  implicit val activeGroupJson: OFormat[ActiveGroup] = Json.format[ActiveGroup]

  def apply(clientDetails: ClientDetails, consumerOffsets: ConsumerOffsetDetails): ActiveGroup = new ActiveGroup(clientDetails, consumerOffsets)
}