package controllers

import javax.inject._
import kafka.coordinator.group.ALIAS._
import kafka.coordinator.group.{ActiveGroup, ConsumerInstanceDetails}
import kafka.security.auth.SimpleAclAuthorizer
import kafkastreams._
import org.apache.kafka.streams.KafkaStreams.State
import org.apache.kafka.streams.kstream.Windowed
import org.apache.kafka.streams.state.QueryableStoreTypes
import org.apache.kafka.streams.KeyValue
import play.api.mvc._
import util.{KafkaUtils, ZookeeperConfig}
import play.api.Configuration
import util.KafkaUtils.UserName

import scala.collection.JavaConverters._

/**
 * This controller creates an `Action` to handle HTTP requests to the
 * application's home page.
 */
@Singleton
class ClientController @Inject()(playConfig: Configuration, cc: ControllerComponents, kafka: KafkaTask) extends AbstractController(cc) {

  /**
   * Create an Action to render an HTML page.
   *
   * The configuration in the `routes` file means that this method
   * will be called when the application receives a `GET` request with
   * a path of `/`.
   */

  private def getContentDetails(
                                 iterator: Seq[KeyValue[Windowed[String], ActiveGroup]],
                                 clientId: String
                               ): Map[GroupId, Map[Topic, Set[ConsumerInstanceDetails]]] = {
    iterator
      .filter(_.value.clientDetails.clientId == clientId)
      .map { itr =>
        val groupedByTopic = KafkaUtils.groupPerTopic(itr.value.clientDetails)
        (itr.value.consumerOffsets.group, groupedByTopic)
      }
      .groupBy(_._1).mapValues(v => v.map(_._2).flatten.toMap)
  }

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

  def index(clientId: String) = Action { implicit request: Request[AnyContent] =>
    if(kafka.stream.state() == State.RUNNING) {
      kafka.stream.allMetadataForStore(ConsumerGroupsProcessor.OFFSETS_AND_META_WINDOW_STORE_NAME)
      val offsetsMetaWindowStore =
        kafka.stream.store(
          ConsumerGroupsProcessor.OFFSETS_AND_META_WINDOW_STORE_NAME,
          QueryableStoreTypes.windowStore[String, ActiveGroup]()
        )

      val iterator: Seq[KeyValue[Windowed[String], ActiveGroup]] = offsetsMetaWindowStore.all().asScala.toList
      val details = getContentDetails(iterator, clientId)
      val authorizer = KafkaUtils.authorizer(ZookeeperConfig(playConfig))
      val aclsDetails: Map[(GroupId, Topic), Set[UserName]] = getAcls(authorizer, details)
      val adminAcls: Set[UserName] = KafkaUtils.currentACLS(authorizer, "*", "*")

      Ok(views.html.client(details, clientId, aclsDetails, adminAcls))

    } else {
      Ok("Stream isn't ready")
    }

  }
}

