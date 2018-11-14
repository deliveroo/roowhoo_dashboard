package kafka.coordinator.group

import kafka.common.OffsetAndMetadata
import play.api.libs.json.{Json, OFormat}

final case class ConsumerOffsetDetails(
                                        topic: String,
                                        partition: Int,
                                        group: String,
                                        version: Int,
                                        offset: Long,
                                        metadata: String,
                                        commitTimestamp: Long,
                                        expireTimestamp: Long
                                      )

object ConsumerOffsetDetails {
  implicit val detailsJson: OFormat[ConsumerOffsetDetails] = Json.format[ConsumerOffsetDetails]

  def apply(k: OffsetKey, value: OffsetAndMetadata): ConsumerOffsetDetails = {
    ConsumerOffsetDetails(
      k.key.topicPartition.topic(),
      k.key.topicPartition.partition(),
      k.key.group,
      k.version,
      value.offset,
      value.metadata,
      value.commitTimestamp,
      value.expireTimestamp)
  }
}