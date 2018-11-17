package controllers

import java.time.{ZoneId, ZonedDateTime}

import javax.inject._
import models.{ActiveGroup, GroupId}
import org.apache.kafka.streams.KafkaStreams.State
import org.apache.kafka.streams.kstream.Windowed
import org.apache.kafka.streams.state.QueryableStoreTypes
import org.apache.kafka.streams.KeyValue
import play.api.Configuration
import play.api.mvc._
import services.stream.GroupMetadataTopic.GroupMetadataStreamTask
import util._

import scala.collection.JavaConverters._

@Singleton
class DashboardController @Inject()(playConfig: Configuration,
                                    cc: ControllerComponents,
                                    kafka: GroupMetadataStreamTask) extends AbstractController(cc) {

  private val STORENAME =
    StreamConfig.OFFSETS_AND_META_WINDOW_STORE_NAME(StreamConfig(playConfig))

  def inbox()  = Action { implicit request: Request[AnyContent] =>
    kafka.stream.state() match {
      case State.RUNNING =>
        kafka.stream.allMetadataForStore(STORENAME)
        val offsetsMetaWindowStore =
          kafka.stream.store(
            STORENAME,
            QueryableStoreTypes.windowStore[GroupId, ActiveGroup]()
          )

        val iterator: Seq[KeyValue[Windowed[GroupId], ActiveGroup]] = offsetsMetaWindowStore.all().asScala.toList

        val todayActiveGroups = KafkaUtils.getLatestStores(iterator).filter(v => {
          val utcZone = ZoneId.of("UTC")
          val todayUTC = ZonedDateTime.now(utcZone)
            .`with`(java.time.temporal.ChronoField.HOUR_OF_DAY, 0)
            .`with`( java.time.temporal.ChronoField.MINUTE_OF_HOUR, 0)
            .`with`( java.time.temporal.ChronoField.SECOND_OF_MINUTE, 0)
            .`with`( java.time.temporal.ChronoField.MICRO_OF_SECOND, 0)

          v.key.window().start >  todayUTC.toInstant.toEpochMilli
        })
        val activeGroups = Content.groupWindowedActiveGroupByClientDetails(todayActiveGroups)

        Ok(views.html.dashboard(activeGroups))

      case State.ERROR => InternalServerError("ERROR")
      case _ =>
        Ok(views.html.loading())

    }
  }
}