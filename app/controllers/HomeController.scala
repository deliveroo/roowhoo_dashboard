package controllers

import java.time.Instant

import javax.inject._
import kafka.coordinator.group.ActiveGroup
import services.kafkastreams._
import org.apache.kafka.streams.KafkaStreams.State
import org.apache.kafka.streams.kstream.Windowed
import org.apache.kafka.streams.state.QueryableStoreTypes
import org.apache.kafka.streams.{KafkaStreams, KeyValue}
import play.api.Configuration
import play.api.mvc._
import util._

import scala.collection.JavaConverters._

@Singleton
class HomeController @Inject()(playConfig: Configuration,
                               cc: ControllerComponents,
                               kafka: KafkaTask) extends AbstractController(cc) {

  private val STORENAME =
    StreamConfig.OFFSETS_AND_META_WINDOW_STORE_NAME(StreamConfig(playConfig))

  private def getWindowsBetween(streams: KafkaStreams, from: Long, to: Long) = {
    val offsetsMetaWindowStore = streams.store(STORENAME,
      QueryableStoreTypes.windowStore[String, ActiveGroup]())
    offsetsMetaWindowStore.fetchAll(from, to)
  }

  def index() = Action { implicit request: Request[AnyContent] =>
    kafka.stream.state() match {
      case State.RUNNING =>
        kafka.stream.allMetadataForStore(STORENAME)
        val offsetsMetaWindowStore =
          kafka.stream.store(
            STORENAME,
            QueryableStoreTypes.windowStore[String, ActiveGroup]()
          )

        val iterator: Seq[KeyValue[Windowed[String], ActiveGroup]] = offsetsMetaWindowStore.all().asScala.toList
        Ok(views.html.index(ContentUtils.groupWindowedActiveGroupByClientDetails(KafkaUtils.getLatestStores(iterator))))

      case State.ERROR => InternalServerError("ERROR")
      case _ =>
        Ok(views.html.loading())

    }
  }

  def lastFiveMinutes() = Action { implicit request: Request[AnyContent] =>
    kafka.stream.state() match {
      case State.RUNNING =>
        val now = Instant.now()
        val fiveMinsAgo = now.minusSeconds(300L)

        kafka.stream.allMetadataForStore(STORENAME)
        val iterator: Seq[KeyValue[Windowed[String], ActiveGroup]] =
          getWindowsBetween(kafka.stream, fiveMinsAgo.toEpochMilli, now.toEpochMilli).asScala.toList
        Ok(views.html.between(ContentUtils.groupWindowedActiveGroupByClientDetails(iterator), fiveMinsAgo.toEpochMilli, now.toEpochMilli))

      case State.ERROR => InternalServerError("ERROR")
      case _ => Ok(views.html.loading())
    }

  }

  def between(from: Long, to: Long) = Action { implicit request: Request[AnyContent] =>
    kafka.stream.state() match {
      case State.RUNNING =>
        kafka.stream.allMetadataForStore(STORENAME)
        val iterator: Seq[KeyValue[Windowed[String], ActiveGroup]] =
          getWindowsBetween(kafka.stream, from, to).asScala.toList
        Ok(views.html.between(ContentUtils.groupWindowedActiveGroupByClientDetails(iterator), from, to))
      case State.ERROR => InternalServerError("ERROR")
      case _ => Ok(views.html.loading())
    }
  }
}

