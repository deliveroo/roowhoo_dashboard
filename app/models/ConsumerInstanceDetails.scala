package models

final case class ConsumerInstanceDetails(
                                   id: ConsumerInstanceId,
                                   host: HostIP,
                                   rebalanceTimeoutMs: Long,
                                   sessionTimeoutMs:Long,
                                   protocolType:Set[String],
                                   protocols:Set[String],
                                   assignedPartitions: Set[(TopicName, PartitionNumber)])
