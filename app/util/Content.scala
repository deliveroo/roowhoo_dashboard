package util

import models._
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.kstream.Windowed

object Content {

  def groupWindowedActiveGroupByClientDetails(iterator: Seq[KeyValue[Windowed[GroupId], ActiveGroup]]
                       ): Seq[(Windowed[GroupId], ClientDetails, Map[TopicName, Set[ConsumerInstanceDetails]])] = {
    iterator.map(itr => {
      val consumerPerTopic= KafkaUtils.groupPerTopic(itr.value.clientDetails)
      val window = itr.key
      val clientDetails = itr.value.clientDetails
      (window, clientDetails, consumerPerTopic)
    }).groupBy({case(_,clientDetails,_) => clientDetails.group})
      .filterKeys(g => !g.startsWith("_"))
      .map(_._2).toSeq.flatten.sortWith({ case (a, b) =>
      a._1.window().start > b._1.window().start
    })
  }

  def groupWindowedActiveGroupByGroupId(
                                         iterator: Seq[KeyValue[Windowed[GroupId], ActiveGroup]],
                                         clientId: ClientId
                                       ): Map[GroupId, Map[TopicName, Set[ConsumerInstanceDetails]]] = {
    KafkaUtils.getLatestStores(iterator)
      .filter(_.value.clientDetails.clientId == clientId)
      .map { itr =>
        val groupedByTopic = KafkaUtils.groupPerTopic(itr.value.clientDetails)
        (itr.value.consumerOffsets.group, groupedByTopic)
      }
      .foldLeft(Map.empty[GroupId, Map[TopicName, Set[ConsumerInstanceDetails]]])((acc, v) => acc.get(v._1) match {
        case Some(topicDetails: Map[TopicName, Set[ConsumerInstanceDetails]]) =>
          val topicToDetailses = topicDetails.foldLeft(Map.empty[TopicName, Set[ConsumerInstanceDetails]])((tAcc, topic) => tAcc.get(topic._1) match  {
            case None => tAcc + topic
            case Some(t) =>
              val detailses = t ++ topic._2
              tAcc + (topic._1 -> detailses)
          })
          acc + (v._1 -> topicToDetailses)
        case None => acc + (v._1 -> v._2)
      })
  }


}
