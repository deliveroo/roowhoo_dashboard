package services.stream.GroupMetadataTopic

import play.api.inject.{SimpleModule, _}
import javax.inject.Inject
import akka.actor.ActorSystem
import org.apache.kafka.streams.KafkaStreams
import play.api.Configuration
import util.StreamConfig

import scala.concurrent.{ExecutionContext, Future}

class GroupMetadataStreamAct extends SimpleModule(bind[GroupMetadataStreamTask].toSelf.eagerly())

class GroupMetadataStreamTask @Inject()(actorSystem: ActorSystem, playConfig: Configuration)(lifecycle: ApplicationLifecycle)(implicit executionContext: ExecutionContext) {

  val stream: KafkaStreams  = StreamGroupMetadata.stream(StreamConfig(playConfig))

  actorSystem.registerOnTermination { () =>
      Future.successful({
        StreamGroupMetadata.shutdown(stream)
      })
    }


}