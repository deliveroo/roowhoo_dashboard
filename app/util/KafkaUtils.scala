package util

import java.net.InetAddress
import models._
import kafka.security.auth.SimpleAclAuthorizer
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.kstream.Windowed

import scala.collection.JavaConverters._

object KafkaUtils {
  type UserName = String

  private def zookeeperConnectionURI(zookeeperConfig: ZookeeperConfig): String = {
    val zkHost = InetAddress.getByName(zookeeperConfig.host).getHostAddress
    s"${zkHost}:${zookeeperConfig.port}"
  }

  def getLatestStores(iterator: Seq[KeyValue[Windowed[GroupId], ActiveGroup]]):
  Seq[KeyValue[Windowed[String], ActiveGroup]] = {
    iterator
      .groupBy {
        case(keyValuePair) => keyValuePair.value.clientDetails.group
      }.mapValues(s =>
      s.sortWith({case(a,b) =>
        a.key.window().start > b.key.window().start
      }
      ).headOption.toSeq
    ).values.toSeq.flatten
  }


  def groupPerTopic(clientDetails: ClientDetails):  Map[TopicName, Set[ConsumerInstanceDetails]] = {
    clientDetails.members.flatMap(m =>
      m.assignedPartitions.map(_._1).map(_ -> m)
    ).groupBy(_._1).mapValues(v=> v.map(_._2))
  }

  def authorizer(zookeeperConfig: ZookeeperConfig): SimpleAclAuthorizer = {
    val zkHost = InetAddress.getByName(zookeeperConfig.host).getHostAddress
    new SimpleAclAuthorizer() {
      configure(Map("zookeeper.connect" -> s"$zkHost:${zookeeperConfig.port}").asJava)
    }
  }

  def currentACLS(authorizer: SimpleAclAuthorizer, topic: TopicName, groupId: GroupId):Set[UserName] = {
    val kafkaAcls = authorizer.getAcls()
    for {
      resource <- kafkaAcls.keySet
      if (resource.name == topic || resource.name == groupId)
      acls <- kafkaAcls(resource)
      usr = acls.principal.getName
    } yield usr

  }
}
