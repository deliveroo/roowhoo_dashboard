package kafkastreams

import java.nio.ByteBuffer
import java.time.Instant

import com.typesafe.scalalogging.LazyLogging
import kafka.coordinator.group._
import kafka.coordinator.group.{ClientDetails, ConsumerOffsetDetails}
import org.apache.kafka.streams.kstream.Reducer

object ConsumerOffsetsFn  extends LazyLogging  {
  def isOffset (key:Array[Byte], value:Array[Byte]): Boolean = {
    GroupMetadataManager.readMessageKey(ByteBuffer.wrap(key)) match {
      case _: OffsetKey => true
      case _: GroupMetadataKey => false
    }
  }

  def isGroupMetadata(key:Array[Byte], value:Array[Byte]): Boolean = {
    !isOffset(key, value)
  }
  def isTombstone(k: Array[Byte], v: Array[Byte]): Boolean  =
    v == null

  def isCommittedLastTenMins (k:String ,v: ConsumerOffsetDetails): Boolean =  {
    logger.info(s"####v: ${v}")
    val commitTs = Instant.ofEpochMilli(v.commitTimestamp)
    val now = Instant.now()
    val tenMinutesAgo = now.minusSeconds(600)

    if(commitTs.compareTo(tenMinutesAgo) < 1) false else true
  }

  def offsetConsumerGroupKey(k: Array[Byte], v: Array[Byte]): (String,ConsumerOffsetDetails) =  {
    val offsetKey = GroupMetadataManager.readMessageKey(ByteBuffer.wrap(k)).asInstanceOf[OffsetKey]
    val value = GroupMetadataManager.readOffsetMessageValue(ByteBuffer.wrap(v))
    (offsetKey.key.group, ConsumerOffsetDetails(offsetKey, value))
  }

  def groupMetadataConsumerGroupKey(k: Array[Byte], v: Array[Byte]): (String, ClientDetails)  = {
    val consumerGroup = GroupMetadataManager.readMessageKey(ByteBuffer.wrap(k)).asInstanceOf[GroupMetadataKey].toString
    val clientsDetail = ClientDetails(consumerGroup, v)
    (consumerGroup, clientsDetail)
  }

  val newGroupMetadataAggregateInit = () => ClientDetails.empty
  val newLatestGroupMeta = (consumerGroupName:String, v: ClientDetails, agg: ClientDetails) => {
    if (ClientDetails.isEmpty(agg)) {
      if (v.generationId > agg.generationId) v else agg
    } else v
  }

  def offsetCommitToMetadataValueJoiner(offsetCommit:ConsumerOffsetDetails, groupMetadata:ClientDetails): (ClientDetails, ConsumerOffsetDetails) = {
    (groupMetadata, offsetCommit)
  }

  val offsetCommitToMetaValueReducer: Reducer[ActiveGroup] = (_:ActiveGroup, v2:ActiveGroup) => {
    v2
  }

}
