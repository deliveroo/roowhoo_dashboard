package kafkastreams

import play.api.inject.{SimpleModule, _}
import javax.inject.Inject
import akka.actor.ActorSystem
import org.apache.kafka.streams.KafkaStreams
import play.api.Configuration
import util.Config

import scala.concurrent.{ExecutionContext, Future}

class KafkaStreamsAct extends SimpleModule(bind[KafkaTask].toSelf.eagerly())

class KafkaTask @Inject()(actorSystem: ActorSystem,playConfig: Configuration)(lifecycle: ApplicationLifecycle)(implicit executionContext: ExecutionContext) {

  val stream: KafkaStreams  = ConsumerGroupsProcessor.stream(Config(playConfig))

  actorSystem.registerOnTermination { () =>
      Future.successful({
        println("CLOSE STREAM")
        ConsumerGroupsProcessor.shutdown(stream)
      })
    }


}