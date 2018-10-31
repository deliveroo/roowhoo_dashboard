package util

import kafka.coordinator.group.ALIAS.{ClientId, Topic}
import kafka.coordinator.group.{ClientDetails, ConsumerInstanceDetails}
import org.apache.kafka.streams.kstream.Windowed

object Frontend {

  val riskClientIds = Seq("rdkafka", "ruby-kafka", "consumer-1", "nonode@nohost")

  val internalStreamTopic = "KSTREAM"
  def colorRow(clientDetails: ClientDetails): String  = {
    if(riskClientIds.contains(clientDetails.clientId) || riskClientIds.exists(clientDetails.clientId.startsWith(_))) "text-light risk"
    else {
      if(clientDetails.group.startsWith("_")
        || clientDetails.clientId.startsWith("perf")
        || clientDetails.clientId.startsWith("console")) "text-muted"

      else ""
    }
  }

  def activeTopics(activeGroups: Seq[(Windowed[String], ClientDetails, Map[Topic, Set[ConsumerInstanceDetails]])]): Set[Topic] =
    topicsFromActiveGroups(activeGroups)
      .filterNot(topic=>topic.startsWith("_") || topic.contains(internalStreamTopic))


  private def topicsFromActiveGroups(activeGroups: Seq[(Windowed[String], ClientDetails, Map[Topic, Set[ConsumerInstanceDetails]])]): Set[Topic] = {
    activeGroups
      .flatMap(_._3.keys).toSet
  }

  def internalStreamTopics(activeGroups: Seq[(Windowed[String], ClientDetails, Map[Topic, Set[ConsumerInstanceDetails]])]): Set[Topic] =
    topicsFromActiveGroups(activeGroups).filter(_.contains(internalStreamTopic))

  def clientsWithRiskClientIds(activeGroups: Seq[(Windowed[String], ClientDetails, Map[Topic, Set[ConsumerInstanceDetails]])]): Seq[ClientDetails] =
    activeGroups.filter(ag =>
      riskClientIds.contains(ag._2.clientId) || riskClientIds.exists(ag._2.clientId.startsWith(_))
    ).map(_._2)

}
