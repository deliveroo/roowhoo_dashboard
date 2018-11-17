package controllers

import java.net.URLDecoder

import javax.inject._
import models._
import kafka.security.auth.SimpleAclAuthorizer
import org.apache.kafka.streams.KafkaStreams.State
import org.apache.kafka.streams.state.QueryableStoreTypes
import play.api.mvc._
import util.{Content, KafkaUtils, StreamConfig, ZookeeperConfig}
import play.api.Configuration
import services.stream.GroupMetadataTopic.GroupMetadataStreamTask
import util.KafkaUtils.UserName

import scala.collection.JavaConverters._


@Singleton
class ClientController @Inject()(playConfig: Configuration, cc: ControllerComponents, kafka: GroupMetadataStreamTask) extends AbstractController(cc) {


  private val STORENAME =
    StreamConfig.OFFSETS_AND_META_WINDOW_STORE_NAME(StreamConfig(playConfig))

  private def getAcls(
                       authorizer: SimpleAclAuthorizer,
                       details:  Map[GroupId, Map[TopicName, Set[ConsumerInstanceDetails]]]
                     ): Map[(GroupId, TopicName), Set[UserName]] = {
    details.flatMap {
      case (groupId, topicdetails) => {
        topicdetails.keys.map( t=>
          (groupId, t) -> KafkaUtils.currentACLS(authorizer, t, groupId)
        ).toMap
      }
    }
  }

  def index(enCodedClientId: String) = Action { implicit request: Request[AnyContent] =>
    kafka.stream.state() match {
      case State.RUNNING =>
        kafka.stream.allMetadataForStore(STORENAME)
        val offsetsMetaWindowStore =
          kafka.stream.store(
            STORENAME,
            QueryableStoreTypes.windowStore[GroupId, ActiveGroup]()
          )


        val clientId = URLDecoder.decode(enCodedClientId, "UTF-8")
        val details = Content.groupWindowedActiveGroupByGroupId(
          offsetsMetaWindowStore.all().asScala.toList,
          clientId
        )

        val authorizer = KafkaUtils.authorizer(ZookeeperConfig(playConfig))
        val aclsDetails: Map[(GroupId, TopicName), Set[UserName]] = getAcls(authorizer, details)
        val adminAcls: Set[UserName] = KafkaUtils.currentACLS(authorizer, "*", "*")

        Ok(views.html.client(details, clientId, aclsDetails, adminAcls))

      case State.ERROR => InternalServerError("Error")
      case _ => Ok(views.html.loading())
    }
  }
}

