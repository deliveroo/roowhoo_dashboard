package controllers

import javax.inject._
import kafka.coordinator.group.ActiveGroup
import org.apache.kafka.streams.KafkaStreams.State
import org.apache.kafka.streams.kstream.Windowed
import org.apache.kafka.streams.state.QueryableStoreTypes
import org.apache.kafka.streams.KeyValue
import play.api.Configuration
import play.api.mvc._
import services.stream.GroupMetadataTopic.KafkaTask
import util._

import scala.collection.JavaConverters._

@Singleton
class DashboardController @Inject()(playConfig: Configuration,
                                    cc: ControllerComponents,
                                    kafka: KafkaTask) extends AbstractController(cc) {

  private val STORENAME =
    StreamConfig.OFFSETS_AND_META_WINDOW_STORE_NAME(StreamConfig(playConfig))

  def inbox()  = Action { implicit request: Request[AnyContent] =>
    kafka.stream.state() match {
      case State.RUNNING =>
        kafka.stream.allMetadataForStore(STORENAME)
        val offsetsMetaWindowStore =
          kafka.stream.store(
            STORENAME,
            QueryableStoreTypes.windowStore[String, ActiveGroup]()
          )

        val iterator: Seq[KeyValue[Windowed[String], ActiveGroup]] = offsetsMetaWindowStore.all().asScala.toList

        val activeGroups = Content.groupWindowedActiveGroupByClientDetails(KafkaUtils.getLatestStores(iterator))

        Ok(views.html.dashboard(activeGroups))

      case State.ERROR => InternalServerError("ERROR")
      case _ =>
        Ok(views.html.loading())

    }
  }
}
