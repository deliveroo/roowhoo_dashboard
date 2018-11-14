package kafka.coordinator.group

import kafka.common.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition

object GroupMetadataRecordWrapper {

  def offsetCommitRecordKey(group: String, topicPartition: TopicPartition) = {
    GroupMetadataManager.offsetCommitKey(group, topicPartition)
  }

  def offsetCommitRecordValue(offsetAndMetadata: OffsetAndMetadata) = {
    GroupMetadataManager.offsetCommitValue(offsetAndMetadata)
  }

  def groupMetadataKey(group: String) = {
    GroupMetadataManager.groupMetadataKey(group)
  }
}
