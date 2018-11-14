package controllers

import java.net.URLDecoder

import javax.inject._
import kafka.coordinator.group.{ActiveGroup, ConsumerInstanceDetails, GroupId, Topic}
import kafka.security.auth.SimpleAclAuthorizer
import org.apache.kafka.streams.KafkaStreams.State
import org.apache.kafka.streams.kstream.Windowed
import org.apache.kafka.streams.state.QueryableStoreTypes
import org.apache.kafka.streams.KeyValue
import play.api.mvc._
import util.{Content, KafkaUtils, StreamConfig, ZookeeperConfig}
import play.api.Configuration
import services.stream.GroupMetadataTopic.KafkaTask
import util.KafkaUtils.UserName
import scala.collection.JavaConverters._


@Singleton
class ClientController @Inject()(playConfig: Configuration, cc: ControllerComponents, kafka: KafkaTask) extends AbstractController(cc) {


  private val STORENAME =
    StreamConfig.OFFSETS_AND_META_WINDOW_STORE_NAME(StreamConfig(playConfig))

  private def getAcls(
                       authorizer: SimpleAclAuthorizer,
                       details:  Map[GroupId, Map[Topic, Set[ConsumerInstanceDetails]]]
                     ): Map[(GroupId, Topic), Set[UserName]] = {
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
            QueryableStoreTypes.windowStore[String, ActiveGroup]()
          )


        val clientId = URLDecoder.decode(enCodedClientId, "UTF-8")
        val iterator: Seq[KeyValue[Windowed[String], ActiveGroup]] = offsetsMetaWindowStore.all().asScala.toList
        val details = Content.groupWindowedActiveGroupByGroupId(iterator, clientId)
        val authorizer = KafkaUtils.authorizer(ZookeeperConfig(playConfig))
        val aclsDetails: Map[(GroupId, Topic), Set[UserName]] = getAcls(authorizer, details)
        val adminAcls: Set[UserName] = KafkaUtils.currentACLS(authorizer, "*", "*")

        Ok(views.html.client(details, clientId, aclsDetails, adminAcls))

      case State.ERROR => InternalServerError("Error")
      case _ => Ok(views.html.loading())
    }
  }
}

