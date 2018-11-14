package util

import models._
import org.apache.kafka.streams.kstream.Windowed

object Frontend {

  val riskClientIds = Seq("rdkafka", "ruby-kafka", "nonode@nohost")

  def isRisky(clientId: ClientId): Boolean =
    riskClientIds.contains(clientId) ||
      riskClientIds.exists(clientId.startsWith) ||
      clientId.matches("consumer-\\d+")

  val internalStreamTopicKeyWords: Seq[String] = Seq("KSTREAM", "KSTORE", "-changelog", "-repartition")
  def colorRow(clientDetails: ClientDetails): String  = {
    if(isRisky(clientDetails.clientId)) "text-light risk"
    else {
      if(clientDetails.group.startsWith("_")
        || clientDetails.clientId.startsWith("perf")
        || clientDetails.clientId.startsWith("console")) "text-muted"

      else ""
    }
  }

  def activeTopics(activeGroups: Seq[(Windowed[String], ClientDetails, Map[TopicName, Set[ConsumerInstanceDetails]])]): Set[TopicName] =
    topicsFromActiveGroups(activeGroups)
      .filterNot(topic=>internalTopic(topic) || streamTopic(topic))


  private def topicsFromActiveGroups(activeGroups: Seq[(Windowed[String], ClientDetails, Map[TopicName, Set[ConsumerInstanceDetails]])]): Set[TopicName] = {
    activeGroups
      .flatMap(_._3.keys).toSet
  }

  def internalStreamTopics(activeGroups: Seq[(Windowed[String], ClientDetails, Map[TopicName, Set[ConsumerInstanceDetails]])]): Set[TopicName] =
    topicsFromActiveGroups(activeGroups).filter(streamTopic)

  def clientsWithRiskClientIds(activeGroups: Seq[(Windowed[String], ClientDetails, Map[TopicName, Set[ConsumerInstanceDetails]])]): Seq[ClientDetails] =
    activeGroups.filter(ag =>
      isRisky(ag._2.clientId)
    ).map(_._2)


  def internalTopic(name: String): Boolean = name.startsWith("_") && !streamTopic(name)

  def streamTopic(name: String): Boolean =
    internalStreamTopicKeyWords.exists(name.startsWith)
}
