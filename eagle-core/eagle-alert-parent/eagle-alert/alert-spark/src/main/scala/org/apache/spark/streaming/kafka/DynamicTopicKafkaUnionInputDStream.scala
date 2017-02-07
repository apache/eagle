/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one or more
 *  * contributor license agreements.  See the NOTICE file distributed with
 *  * this work for additional information regarding copyright ownership.
 *  * The ASF licenses this file to You under the Apache License, Version 2.0
 *  * (the "License"); you may not use this file except in compliance with
 *  * the License.  You may obtain a copy of the License at
 *  *
 *  *    http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */
package org.apache.spark.streaming.kafka

import java.lang.{Long => JLong}

import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.Decoder
import org.apache.eagle.alert.engine.spark.model.KafkaClusterInfo
import org.apache.spark.SparkException
import org.apache.spark.internal.Logging
import org.apache.spark.streaming.dstream._
import org.apache.spark.streaming.kafka.KafkaCluster.LeaderOffset
import org.apache.spark.streaming.scheduler.{RateController, StreamInputInfo}
import org.apache.spark.streaming.scheduler.rate.RateEstimator
import org.apache.spark.streaming.{StreamingContext, Time}

import scala.annotation.tailrec
import scala.collection.immutable.Map
import scala.collection.mutable
import scala.reflect.ClassTag
import scala.collection.JavaConverters._

private[streaming]
class DynamicTopicKafkaUnionInputDStream[
K: ClassTag,
V: ClassTag,
U <: Decoder[K] : ClassTag,
T <: Decoder[V] : ClassTag,
R: ClassTag](
              _ssc: StreamingContext,
              val clusterInfo: List[KafkaClusterInfo],
              refreshClusterAndTopicHandler: List[KafkaClusterInfo] => List[KafkaClusterInfo],
              getOffsetRangeHandler: (Array[OffsetRange], KafkaClusterInfo) => Unit,
              messageHandler: MessageAndMetadata[K, V] => R
            ) extends InputDStream[R](_ssc) with Logging {

  val maxRetries = context.sparkContext.getConf.getInt(
    "spark.streaming.kafka.maxRetries", 1)
  private[streaming] override def name: String = s"Kafka direct stream [$id]"

  protected[streaming] override val checkpointData =
    new DirectKafkaInputDStreamCheckpointData

  private val maxRateLimitPerPartition: Int = context.sparkContext.getConf.getInt(
    "spark.streaming.kafka.maxRatePerPartition", 0)

  protected var currentClusterInfo = List[KafkaClusterInfo]()

  override def compute(validTime: Time): Option[KafkaRDD[K, V, U, T, R]] = {
    // refresh topic and cluster
    currentClusterInfo = refreshClusterAndTopicHandler(currentClusterInfo)
    val rdds = currentClusterInfo.map{ kafkaClusterInfo  =>
      val fromOffsets = Map(kafkaClusterInfo.getOffsets.asScala.mapValues(_.longValue()).toSeq: _*)
      var kafkaParams = Map(kafkaClusterInfo.getKafkaParams.asScala.toSeq: _*)
      val kc = kafkaClusterInfo.getKafkaCluster
      // get until offsets
      val untilOffsets = clamp(latestLeaderOffsets(maxRetries, kc, fromOffsets), fromOffsets)
      val rdd = KafkaRDD[K, V, U, T, R](
        context.sparkContext, kafkaParams, fromOffsets, untilOffsets, messageHandler)
      val offsetRanges = fromOffsets.map { case (tp, fo) =>
        val uo = untilOffsets(tp)
        OffsetRange(tp.topic, tp.partition, fo, uo.offset)
      }
      val description = offsetRanges.filter { offsetRange =>
        // Don't display empty ranges.
        offsetRange.fromOffset != offsetRange.untilOffset
      }.map { offsetRange =>
        s"topic: ${offsetRange.topic}\tpartition: ${offsetRange.partition}\t" +
          s"offsets: ${offsetRange.fromOffset} to ${offsetRange.untilOffset}"
      }.mkString("\n")
      val metadata = Map(
        "offsets" -> offsetRanges.toList,
        StreamInputInfo.METADATA_KEY_DESCRIPTION -> description)
      // val inputInfo = StreamInputInfo(id, rdd.count, metadata)
      // ssc.scheduler.inputInfoTracker.reportInfo(validTime, inputInfo)
      // refresh currentoffset map
      kafkaClusterInfo.setOffsets(untilOffsets.map(kv => kv._1 -> kv._2.offset.asInstanceOf[JLong]).asJava)
      getOffsetRangeHandler(rdd.offsetRanges, kafkaClusterInfo)
      rdd
    }
    // return union kafkardd
    var unionRdd = rdds.head
    rdds.filter(rdd => rdd != unionRdd).foreach(rdd => unionRdd.union(rdd))
    return Some(unionRdd)
  }

  override def start(): Unit = {
  }

  def stop(): Unit = {
  }

  protected[streaming] def maxMessagesPerPartition(
                                                    offsets: Map[TopicAndPartition, Long],
                                                    currentOffsets: Map[TopicAndPartition, Long]): Option[Map[TopicAndPartition, Long]] = {
    val estimatedRateLimit = rateController.map(_.getLatestRate().toInt)

    // calculate a per-partition rate limit based on current lag
    val effectiveRateLimitPerPartition = estimatedRateLimit.filter(_ > 0) match {
      case Some(rate) =>
        val lagPerPartition = offsets.map { case (tp, offset) =>
          tp -> Math.max(offset - currentOffsets(tp), 0)
        }
        val totalLag = lagPerPartition.values.sum

        lagPerPartition.map { case (tp, lag) =>
          val backpressureRate = Math.round(lag / totalLag.toFloat * rate)
          tp -> (if (maxRateLimitPerPartition > 0) {
            Math.min(backpressureRate, maxRateLimitPerPartition)
          } else backpressureRate)
        }
      case None => offsets.map { case (tp, offset) => tp -> maxRateLimitPerPartition }
    }

    if (effectiveRateLimitPerPartition.values.sum > 0) {
      val secsPerBatch = context.graph.batchDuration.milliseconds.toDouble / 1000
      Some(effectiveRateLimitPerPartition.map {
        case (tp, limit) => tp -> (secsPerBatch * limit).toLong
      })
    } else {
      None
    }
  }

  @tailrec
  protected final def latestLeaderOffsets(retries: Int, kc: KafkaCluster, offsets: Map[TopicAndPartition, Long]): Map[TopicAndPartition, LeaderOffset] = {
    val o = kc.getLatestLeaderOffsets(offsets.keySet)
    // Either.fold would confuse @tailrec, do it manually
    if (o.isLeft) {
      val err = o.left.get.toString
      if (retries <= 0) {
        throw new SparkException(err)
      } else {
        log.error(err)
        Thread.sleep(kc.config.refreshLeaderBackoffMs)
        latestLeaderOffsets(retries - 1, kc, offsets)
      }
    } else {
      o.right.get
    }
  }

  // limits the maximum number of messages per partition
  protected def clamp(
                       leaderOffsets: Map[TopicAndPartition, LeaderOffset], currentOffsets: Map[TopicAndPartition, Long]): Map[TopicAndPartition, LeaderOffset] = {
    val offsets = leaderOffsets.mapValues(lo => lo.offset)

    maxMessagesPerPartition(offsets, currentOffsets).map { mmp =>
      mmp.map { case (tp, messages) =>
        val lo = leaderOffsets(tp)
        tp -> lo.copy(offset = Math.min(currentOffsets(tp) + messages, lo.offset))
      }
    }.getOrElse(leaderOffsets)
  }

  /**
    * Asynchronously maintains & sends new rate limits to the receiver through the receiver tracker.
    */
  override protected[streaming] val rateController: Option[RateController] = {
    if (RateController.isBackPressureEnabled(ssc.conf)) {
      Some(new DirectKafkaRateController(id,
        RateEstimator.create(ssc.conf, context.graph.batchDuration)))
    } else {
      None
    }
  }

  private[streaming]
  class DirectKafkaInputDStreamCheckpointData extends DStreamCheckpointData(this) {
    def batchForTime: mutable.HashMap[Time, Array[(String, Int, Long, Long)]] = {
      data.asInstanceOf[mutable.HashMap[Time, Array[OffsetRange.OffsetRangeTuple]]]
    }

    override def update(time: Time) {
      batchForTime.clear()
      generatedRDDs.foreach { kv =>
        val a = kv._2.asInstanceOf[KafkaRDD[K, V, U, T, R]].offsetRanges.map(_.toTuple).toArray
        batchForTime += kv._1 -> a
      }
    }

    override def cleanup(time: Time) {}

    override def restore() {
      // this is assuming that the topics don't change during execution, which is true currently
      currentClusterInfo.foreach { clusterInfo =>
        val offsets = Map(clusterInfo.getOffsets.asScala.mapValues(_.longValue()).toSeq: _*)
        val topics = offsets.keySet
        val leaders = KafkaCluster.checkErrors(clusterInfo.getKafkaCluster.findLeaders(topics))
        batchForTime.toSeq.sortBy(_._1)(Time.ordering).foreach { case (t, b) =>
          logInfo(s"Restoring KafkaRDD for time $t ${b.mkString("[", ", ", "]")}")
          generatedRDDs += t -> new KafkaRDD[K, V, U, T, R](
            context.sparkContext, Map(clusterInfo.getKafkaParams.asScala.toSeq: _*), b.map(OffsetRange(_)), leaders, messageHandler)
        }
      }
    }
  }

  /**
    * A RateController to retrieve the rate from RateEstimator.
    */
  private[streaming] class DirectKafkaRateController(id: Int, estimator: RateEstimator)
    extends RateController(id, estimator) {
    override def publish(rate: Long): Unit = ()
  }
}
