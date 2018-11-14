package controllers

import java.time.Instant

import com.typesafe.scalalogging.LazyLogging
import javax.inject._
import kafka.coordinator.group.ALIAS.Topic
import kafka.coordinator.group.{ActiveGroup, ClientDetails, ConsumerInstanceDetails}
import kafkastreams._
import org.apache.kafka.streams.KafkaStreams.State
import org.apache.kafka.streams.kstream.Windowed
import org.apache.kafka.streams.state.{KeyValueIterator, QueryableStoreTypes}
import org.apache.kafka.streams.{KafkaStreams, KeyValue}
import play.api.Configuration
import play.api.mvc._
import util.{Config, KafkaUtils}

import scala.collection.JavaConverters._

@Singleton
class HomeController @Inject()(playConfig: Configuration, cc: ControllerComponents, kafka: KafkaTask) extends AbstractController(cc) with LazyLogging {


  private def getContentDetails(iterator: Seq[KeyValue[Windowed[String], ActiveGroup]]
                               ): Seq[(Windowed[String], ClientDetails, Map[Topic, Set[ConsumerInstanceDetails]])] = {
    iterator.map(itr => {
      val consumerPerTopic = KafkaUtils.groupPerTopic(itr.value.clientDetails)
      val window = itr.key
      val clientDetails = itr.value.clientDetails
      (window, clientDetails, consumerPerTopic)
    }).groupBy({ case (_, clientDetails, _) => clientDetails.group })
      .filterKeys(g => !g.startsWith("_"))
      .map(_._2).toSeq.flatten.sortWith({ case (a, b) =>
      a._1.window().start < b._1.window().start
    })
  }

  private val STORENAME =
    ConsumerGroupsProcessor.OFFSETS_AND_META_WINDOW_STORE_NAME(Config(playConfig))

  private def getWindowsBetween(streams: KafkaStreams, from: Long, to: Long) = {
    val offsetsMetaWindowStore = streams.store(STORENAME,
      QueryableStoreTypes.windowStore[String, ActiveGroup]())

    val iterator: KeyValueIterator[Windowed[String], ActiveGroup] = offsetsMetaWindowStore.fetchAll(from, to)
    iterator
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
        Ok(views.html.index(getContentDetails(KafkaUtils.getLatestStores(iterator))))

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
        kafka.stream.store(
          STORENAME,
          QueryableStoreTypes.windowStore[String, ActiveGroup]()
        )

        val iterator: Seq[KeyValue[Windowed[String], ActiveGroup]] =
          getWindowsBetween(kafka.stream, fiveMinsAgo.toEpochMilli, now.toEpochMilli).asScala.toList
        Ok(views.html.between(getContentDetails(iterator), fiveMinsAgo.toEpochMilli, now.toEpochMilli))

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
        Ok(views.html.between(getContentDetails(iterator), from, to))
      case State.ERROR => InternalServerError("ERROR")
      case _ => Ok(views.html.loading())
    }
  }
}

