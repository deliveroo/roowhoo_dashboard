package com.deliveroo.kafka.consumer.offsets.viewer

import java.nio.ByteBuffer
import java.time.Instant

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Directives.{complete, get, path, _}
import akka.http.scaladsl.server.Route
import com.deliveroo.kafka.consumer.offsets.viewer.ConsumerGroupsProcessor.OFFSETS_AND_META_WINDOW_STORE_NAME
import com.typesafe.scalalogging.LazyLogging
import kafka.coordinator.group.{ActiveGroup, GroupMetadataKey, GroupMetadataManager, OffsetKey}
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.KafkaStreams.State
import org.apache.kafka.streams.kstream.Windowed
import org.apache.kafka.streams.state.{KeyValueIterator, QueryableStoreTypes}

import scala.collection.JavaConverters._

object ConsumerGroupsEndpoints extends LazyLogging {

  def routesFor(streams: KafkaStreams): Route = {
    path("all") {
      get {
        if(streams.state() == State.RUNNING) {
          streams.allMetadataForStore(OFFSETS_AND_META_WINDOW_STORE_NAME)
          val offsetsMetaWindowStore = streams.store(OFFSETS_AND_META_WINDOW_STORE_NAME, QueryableStoreTypes.windowStore[String, ActiveGroup]())

          val iterator: KeyValueIterator[Windowed[String], ActiveGroup] = offsetsMetaWindowStore.all()

          complete {
            s"${iterateThenClose[Windowed[String], ActiveGroup](iterator)(windowedRecordAsString)}"
          }
        } else complete( StatusCodes.InternalServerError -> "STREAM ISN'T RUNNING NOW" )

      }
    } ~
    path("last-five-minutes") {
      get {
        val now = Instant.now()
        val fiveMinsAgo = now.minusSeconds(300L)
        val iterator: KeyValueIterator[Windowed[String], ActiveGroup] = getWindowsBetween(streams, fiveMinsAgo.toEpochMilli, now.toEpochMilli)

        complete {
          s"${iterateThenClose[Windowed[String], ActiveGroup](iterator)(windowedRecordAsString)}"
        }
      }
    } ~
    path("between" / Segment / Segment) { (from, to) =>
      get {
        val iterator: KeyValueIterator[Windowed[String], ActiveGroup] = getWindowsBetween(streams, from.toLong, to.toLong)

        complete {
          s"${iterateThenClose[Windowed[String], ActiveGroup](iterator)(windowedRecordAsString)}"
        }
      }
    }

  }

  private def getWindowsBetween(streams: KafkaStreams, from: Long, to: Long) = {
    val offsetsMetaWindowStore = streams.store(OFFSETS_AND_META_WINDOW_STORE_NAME, QueryableStoreTypes.windowStore[String, ActiveGroup]())

    val iterator: KeyValueIterator[Windowed[String], ActiveGroup] = offsetsMetaWindowStore.fetchAll(from, to)
    iterator
  }

  private def iterateThenClose[K, V](iterator: KeyValueIterator[K, V]) = {
    (runThrough: (KeyValueIterator[K, V]) => String) => {
      try {
        runThrough(iterator)
      } finally {
        iterator.close()
      }
    }
  }

  private def recordAsString[K](iterator: KeyValueIterator[K, String]) = {
    iterator.asScala
      .map(kv => s"key: ${kv.key}, value: ${kv.value}")
      .mkString("\n")
  }

  private def windowedRecordAsString(iterator: KeyValueIterator[Windowed[String], ActiveGroup]) = {
//    iterator.asScala
//      .map(kv => s"key: ${kv.key}, window start: ${Instant.ofEpochMilli(kv.key.window().start())}, window end: ${Instant.ofEpochMilli(kv.key.window().end())}, value: ${kv.value}")
//      .mkString("\n")
iterator.asScala.toList.mkString(",\n")
//    iterator.asScala
//      .toList
//      .map(kv => (kv.key.key(), kv))
//      .groupBy(consumerGroupToWindow => {
//        val consumerGroup = consumerGroupToWindow._1
//        consumerGroup
//      })
//      .map{ groupToGroupAndWindows => {
//          val consumerGroupWindows = groupToGroupAndWindows._2
//            .map(groupAndWindow => {
//              val groupWindow = groupAndWindow._2
//              s"window start: ${Instant.ofEpochMilli(groupWindow.key.window().start())}, window end: ${Instant.ofEpochMilli(groupWindow.key.window().end())}, window value: ${groupWindow.value} \n \n"
//            }).mkString("\n")
//          s"consumergroup: ${groupToGroupAndWindows._1} \n" +
//          s" ${consumerGroupWindows} \n " +
//          s"----------------------------------------------------------------- \n"
//      }
//      }.mkString("\n")
  }

  private def offsetsAsString(iterator: KeyValueIterator[Array[Byte], Array[Byte]]) = {
    iterator.asScala
      .toList
      .zipWithIndex
      .map {
        case (offsetRecord, index) => {
          val key = GroupMetadataManager.readMessageKey(ByteBuffer.wrap(offsetRecord.key))
          val keyAsString = s"offsetKey: ${key}, "
          val valAsString = key match {
            case _: OffsetKey => "offsetVal: " + GroupMetadataManager.readOffsetMessageValue(ByteBuffer.wrap(offsetRecord.value))
            case groupMeta: GroupMetadataKey => "groupMetaVal: " + GroupMetadataManager.readGroupMessageValue(groupMeta.key, ByteBuffer.wrap(offsetRecord.value))
          }
          Seq(s"${index + 1}: ", keyAsString, valAsString).mkString("")
        }
      }
      .mkString("\n")
  }
}
