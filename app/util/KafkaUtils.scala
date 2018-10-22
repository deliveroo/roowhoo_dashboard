package util

import kafka.coordinator.group.ALIAS.Topic
import kafka.coordinator.group.{ClientDetails, ConsumerInstanceDetails}

object KafkaUtils {
  def groupPerTopic(clientDetails: ClientDetails):  Map[Topic, Set[ConsumerInstanceDetails]] = {
    clientDetails.members.flatMap(m =>
      m.assignedPartitions.map(_._1).map(_ -> m)
    ).groupBy(_._1).mapValues(v=> v.map(_._2))
  }
}
