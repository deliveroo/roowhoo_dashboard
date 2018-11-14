package controllers

import javax.inject._
import java.time.Instant

import kafka.coordinator.group.ALIAS.Topic
import kafka.coordinator.group.{ActiveGroup, ClientDetails, ConsumerInstanceDetails}
import kafkastreams._
import play.api.mvc._
import org.apache.kafka.streams.KafkaStreams.State
import org.apache.kafka.streams.{KafkaStreams, KeyValue}
import org.apache.kafka.streams.kstream.Windowed
import org.apache.kafka.streams.state.{KeyValueIterator, QueryableStoreTypes}

import scala.collection.JavaConverters._

/**
 * This controller creates an `Action` to handle HTTP requests to the
 * application's home page.
 */
@Singleton
class HomeController @Inject()(cc: ControllerComponents, kafka: KafkaTask) extends AbstractController(cc) {

  /**
   * Create an Action to render an HTML page.
   *
   * The configuration in the `routes` file means that this method
   * will be called when the application receives a `GET` request with
   * a path of `/`.
   */

  private def getContentDetails(
                                 iterator: Seq[KeyValue[Windowed[String], ActiveGroup]]
                               ): Seq[KeyValue[Windowed[String], (ClientDetails, Map[Topic, Set[ConsumerInstanceDetails]])]] = {
    iterator.map(itr => {
      val consumerPerTopic: Map[Topic, Set[ConsumerInstanceDetails]] = itr.value.clientDetails.members.flatMap(m =>
        m.assignedPartitions.map(_._1).map(_ -> m)
      ).groupBy(_._1).mapValues(v=> v.map(_._2))
      KeyValue.pair(itr.key, (itr.value.clientDetails, consumerPerTopic))

    })
  }

  private def getWindowsBetween(streams: KafkaStreams, from: Long, to: Long) = {
    val offsetsMetaWindowStore = streams.store(ConsumerGroupsProcessor.OFFSETS_AND_META_WINDOW_STORE_NAME, QueryableStoreTypes.windowStore[String, ActiveGroup]())

    val iterator: KeyValueIterator[Windowed[String], ActiveGroup] = offsetsMetaWindowStore.fetchAll(from, to)
    iterator
  }
  def index() = Action { implicit request: Request[AnyContent] =>
    if(kafka.stream.state() == State.RUNNING) {
      kafka.stream.allMetadataForStore(ConsumerGroupsProcessor.OFFSETS_AND_META_WINDOW_STORE_NAME)
      val offsetsMetaWindowStore =
        kafka.stream.store(
          ConsumerGroupsProcessor.OFFSETS_AND_META_WINDOW_STORE_NAME,
          QueryableStoreTypes.windowStore[String, ActiveGroup]()
        )

      val iterator: Seq[KeyValue[Windowed[String], ActiveGroup]] = offsetsMetaWindowStore.all().asScala.toList
      Ok(views.html.index(getContentDetails(iterator)))

    } else {
      Ok(views.html.index(getContentDetails(Seq[KeyValue[Windowed[String],ActiveGroup]]())))
    }

  }

  def lastFiveMinutes() = Action { implicit request: Request[AnyContent] =>
    if(kafka.stream.state() == State.RUNNING) {
      val now = Instant.now()
      val fiveMinsAgo = now.minusSeconds(300L)

      kafka.stream.allMetadataForStore(ConsumerGroupsProcessor.OFFSETS_AND_META_WINDOW_STORE_NAME)
      val offsetsMetaWindowStore =
        kafka.stream.store(
          ConsumerGroupsProcessor.OFFSETS_AND_META_WINDOW_STORE_NAME,
          QueryableStoreTypes.windowStore[String, ActiveGroup]()
        )

      val iterator: Seq[KeyValue[Windowed[String], ActiveGroup]] =
        getWindowsBetween(kafka.stream, fiveMinsAgo.toEpochMilli, now.toEpochMilli).asScala.toList
      Ok(views.html.between(getContentDetails(iterator), fiveMinsAgo.toEpochMilli, now.toEpochMilli))

    } else InternalServerError("STREAM ISN'T RUNNING NOW")

  }

  def between(from:Long, to: Long) = Action { implicit request: Request[AnyContent] =>
    if(kafka.stream.state() == State.RUNNING) {

      kafka.stream.allMetadataForStore(ConsumerGroupsProcessor.OFFSETS_AND_META_WINDOW_STORE_NAME)
      val offsetsMetaWindowStore =
        kafka.stream.store(
          ConsumerGroupsProcessor.OFFSETS_AND_META_WINDOW_STORE_NAME,
          QueryableStoreTypes.windowStore[String, ActiveGroup]()
        )

      val iterator: Seq[KeyValue[Windowed[String], ActiveGroup]] =
        getWindowsBetween(kafka.stream, from, to).asScala.toList
      Ok(views.html.between(getContentDetails(iterator), from, to))

    } else InternalServerError("STREAM ISN'T RUNNING NOW")

  }


}
