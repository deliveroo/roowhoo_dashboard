package util

import kafka.coordinator.group.ALIAS.{GroupId, Topic}
import kafka.coordinator.group.{ClientDetails, ConsumerInstanceDetails}
import kafka.security.auth.SimpleAclAuthorizer
import scala.collection.JavaConverters._
import java.net.InetAddress

object KafkaUtils {
  type UserName = String

  def groupPerTopic(clientDetails: ClientDetails):  Map[Topic, Set[ConsumerInstanceDetails]] = {
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

  def currentACLS(authorizer: SimpleAclAuthorizer, topic: Topic, groupId: GroupId):Set[UserName] = {
    val kafkaAcls = authorizer.getAcls()
    for {
      resource <- kafkaAcls.keySet
      if (resource.name == topic || resource.name == groupId)
      acls <- kafkaAcls(resource)
      usr = acls.principal.getName
    } yield usr

  }
}
