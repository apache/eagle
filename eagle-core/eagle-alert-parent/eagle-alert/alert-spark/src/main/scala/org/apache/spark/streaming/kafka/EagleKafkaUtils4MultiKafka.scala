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

import java.lang.{Integer => JInt, Long => JLong}
import java.util.concurrent.TimeUnit
import java.util.{HashMap => JHashMap, List => JList, Map => JMap, Set => JSet}

import kafka.admin.AdminUtils
import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.Decoder
import org.I0Itec.zkclient.ZkClient
import org.apache.eagle.alert.engine.spark.model.KafkaClusterInfo
import org.apache.spark.api.java.function.{Function => JFunction, Function2 => JFunction2}
import org.apache.spark.internal.Logging
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.api.java._
import org.apache.spark.streaming.dstream.InputDStream

import scala.collection.JavaConverters._
import scala.collection.immutable.{Map, Set}
import scala.reflect.ClassTag

object EagleKafkaUtils4MultiKafka extends Logging {
  private val ZK_TIMEOUT_MSEC: Int = TimeUnit.MILLISECONDS.convert(30, TimeUnit.SECONDS).toInt

  /**
    * Create an input stream that directly pulls messages from Kafka Brokers
    * without using any receiver. This stream can guarantee that each message
    * from Kafka is included in transformations exactly once (see points below).
    *
    * Points to note:
    * - No receivers: This stream does not use any receiver. It directly queries Kafka
    * - Offsets: This does not use Zookeeper to store offsets. The consumed offsets are tracked
    * by the stream itself. For interoperability with Kafka monitoring tools that depend on
    * Zookeeper, you have to update Kafka/Zookeeper yourself from the streaming application.
    * You can access the offsets used in each batch from the generated RDDs (see
    * [[org.apache.spark.streaming.kafka.HasOffsetRanges]]).
    * - Failure Recovery: To recover from driver failures, you have to enable checkpointing
    * in the [[StreamingContext]]. The information on consumed offset can be
    * recovered from the checkpoint. See the programming guide for details (constraints, etc.).
    * - End-to-end semantics: This stream ensures that every records is effectively received and
    * transformed exactly once, but gives no guarantees on whether the transformed data are
    * outputted exactly once. For end-to-end exactly-once semantics, you have to either ensure
    * that the output operation is idempotent, or use transactions to output records atomically.
    * See the programming guide for more details.
    *
    * @param ssc                      StreamingContext object
    * @param kafkaParams              Kafka <a href="http://kafka.apache.org/documentation.html#configuration">
    *                                 configuration parameters</a>. Requires "metadata.broker.list" or "bootstrap.servers"
    *                                 to be set with Kafka broker(s) (NOT zookeeper servers) specified in
    *                                 host1:port1,host2:port2 form.
    * @param fromOffsets              Per-topic/partition Kafka offsets defining the (inclusive)
    *                                 starting point of the stream
    * @param topicAndPartitionHandler Chang Map[TopicAndPartition, Long] dynamically to
    *                                 support add/remove topic without restart
    * @param getOffsetRangeHandler get rdd offset
    * @param messageHandler           Function for translating each message and metadata into the desired type
    * @tparam K  type of Kafka message key
    * @tparam V  type of Kafka message value
    * @tparam KD type of Kafka message key decoder
    * @tparam VD type of Kafka message value decoder
    * @tparam R  type returned by messageHandler
    * @return DStream of R
    */
  def createDirectStreamWithHandler[
  K: ClassTag,
  V: ClassTag,
  KD <: Decoder[K] : ClassTag,
  VD <: Decoder[V] : ClassTag,
  R: ClassTag](
                ssc: StreamingContext,
                kafkaParams: Map[String, String],
                fromOffsets: Map[TopicAndPartition, Long],
                topicAndPartitionHandler: Map[TopicAndPartition, Long] => Map[TopicAndPartition, Long],
                getOffsetRangeHandler: (Array[OffsetRange], Map[String, String]) => Array[OffsetRange],
                messageHandler: MessageAndMetadata[K, V] => R
              ): InputDStream[R] = {
    val cleanedHandler = ssc.sc.clean(messageHandler)
    val cleanedTopicAndPartitionHandler = ssc.sc.clean(topicAndPartitionHandler)
    val cleanedGetOffsetRangeHandler = ssc.sc.clean(getOffsetRangeHandler)
    new DynamicTopicKafkaInputDStream4KafkaMultiKafka[K, V, KD, VD, R](
      ssc, kafkaParams, fromOffsets, cleanedTopicAndPartitionHandler, cleanedGetOffsetRangeHandler, cleanedHandler)
  }


  /**
    * Create an input stream that directly pulls messages from Kafka Brokers
    * without using any receiver. This stream can guarantee that each message
    * from Kafka is included in transformations exactly once (see points below).
    *
    * Points to note:
    * - No receivers: This stream does not use any receiver. It directly queries Kafka
    * - Offsets: This does not use Zookeeper to store offsets. The consumed offsets are tracked
    * by the stream itself. For interoperability with Kafka monitoring tools that depend on
    * Zookeeper, you have to update Kafka/Zookeeper yourself from the streaming application.
    * You can access the offsets used in each batch from the generated RDDs (see
    * [[org.apache.spark.streaming.kafka.HasOffsetRanges]]).
    * - Failure Recovery: To recover from driver failures, you have to enable checkpointing
    * in the [[StreamingContext]]. The information on consumed offset can be
    * recovered from the checkpoint. See the programming guide for details (constraints, etc.).
    * - End-to-end semantics: This stream ensures that every records is effectively received and
    * transformed exactly once, but gives no guarantees on whether the transformed data are
    * outputted exactly once. For end-to-end exactly-once semantics, you have to either ensure
    * that the output operation is idempotent, or use transactions to output records atomically.
    * See the programming guide for more details.
    *
    * @param jssc                     JavaStreamingContext object
    * @param keyClass                 Class of the keys in the Kafka records
    * @param valueClass               Class of the values in the Kafka records
    * @param keyDecoderClass          Class of the key decoder
    * @param valueDecoderClass        Class of the value decoder
    * @param recordClass              Class of the records in DStream
    * @param kafkaParams              Kafka <a href="http://kafka.apache.org/documentation.html#configuration">
    *                                 configuration parameters</a>. Requires "metadata.broker.list" or "bootstrap.servers"
    *                                 to be set with Kafka broker(s) (NOT zookeeper servers), specified in
    *                                 host1:port1,host2:port2 form.
    * @param fromOffsets              Per-topic/partition Kafka offsets defining the (inclusive)
    *                                 starting point of the stream
    * @param topicAndPartitionHandler Chang Map[TopicAndPartition, Long] dynamically to
    *                                 support add/remove topic without restart
    * @param getOffsetRangeHandler get rdd offset
    * @param messageHandler           Function for translating each message and metadata into the desired type
    * @tparam K  type of Kafka message key
    * @tparam V  type of Kafka message value
    * @tparam KD type of Kafka message key decoder
    * @tparam VD type of Kafka message value decoder
    * @tparam R  type returned by messageHandler
    * @return DStream of R
    */
  def createDirectStream[K, V, KD <: Decoder[K], VD <: Decoder[V], R](
                                                                       jssc: JavaStreamingContext,
                                                                       keyClass: Class[K],
                                                                       valueClass: Class[V],
                                                                       keyDecoderClass: Class[KD],
                                                                       valueDecoderClass: Class[VD],
                                                                       recordClass: Class[R],
                                                                       kafkaParams: JMap[String, String],
                                                                       fromOffsets: JMap[TopicAndPartition, JLong],
                                                                       topicAndPartitionHandler: JFunction[Map[TopicAndPartition, Long], Map[TopicAndPartition, Long]],
                                                                       getOffsetRangeHandler: JFunction2[Array[OffsetRange], Map[String, String], Array[OffsetRange]],
                                                                       messageHandler: JFunction[MessageAndMetadata[K, V], R]
                                                                     ): JavaInputDStream[R] = {
    implicit val keyCmt: ClassTag[K] = ClassTag(keyClass)
    implicit val valueCmt: ClassTag[V] = ClassTag(valueClass)
    implicit val keyDecoderCmt: ClassTag[KD] = ClassTag(keyDecoderClass)
    implicit val valueDecoderCmt: ClassTag[VD] = ClassTag(valueDecoderClass)
    implicit val recordCmt: ClassTag[R] = ClassTag(recordClass)
    val cleanedHandler = jssc.sparkContext.clean(messageHandler.call _)
    val cleanedTopicAndPartitionHandler = jssc.sparkContext.clean(topicAndPartitionHandler.call _)
    val cleanedGetOffsetRangeHandler = jssc.sparkContext.clean(getOffsetRangeHandler.call _)
    createDirectStreamWithHandler[K, V, KD, VD, R](
      jssc.ssc,
      Map(kafkaParams.asScala.toSeq: _*),
      Map(fromOffsets.asScala.mapValues(_.longValue()).toSeq: _*),
      cleanedTopicAndPartitionHandler,
      cleanedGetOffsetRangeHandler,
      cleanedHandler
    )
  }

  /**
    * @param zkServers Zookeeper server string: host1:port1[,host2:port2,...]
    * @param topic     topic to check for existence
    * @return { @code true} if and only if the given topic exists
    */
  def topicExists(zkServers: String, topic: String): Boolean = {
    val zkClient: ZkClient = new ZkClient(zkServers, ZK_TIMEOUT_MSEC, ZK_TIMEOUT_MSEC);
    try {
      return AdminUtils.topicExists(zkClient, topic);
    } finally {
      zkClient.close;
    }
  }

  def fillInLatestOffsets(topics: JSet[String], fromOffsets: JMap[TopicAndPartition, JLong], groupId: String, kafkaCluster: KafkaCluster, zkServers: String): Unit = {

    if (topics.isEmpty) {
      throw new IllegalArgumentException
    }
    val zkClient: ZkClient = new ZkClient(zkServers, ZK_TIMEOUT_MSEC, ZK_TIMEOUT_MSEC);
    try {
      val effectiveTopics = topics.asScala.filter(topic => AdminUtils.topicExists(zkClient, topic))
      logInfo("effectiveTopics" + effectiveTopics)
      val tp = kafkaCluster.getPartitions(effectiveTopics.toSet).right.get
      tp.foreach(
        eachTp => {
          val result = kafkaCluster.getConsumerOffsets(groupId, Set(eachTp))
          if (result.isLeft) {
            fromOffsets.put(eachTp, 0L)
          } else {
            result.right.get.foreach(tpAndOffset => {
              fromOffsets.put(tpAndOffset._1, tpAndOffset._2)
            })
          }
        }
      )

      logInfo("fillInLatestOffsets fromOffsets" + fromOffsets)
    } finally {
      zkClient.close;
    }
  }

  def fillInLatestOffsetsByCluster(clusterInfoMap: JMap[KafkaClusterInfo, JSet[String]],
                                   fromOffsetsMap: JMap[KafkaClusterInfo, JMap[TopicAndPartition, JLong]],
                                   groupId: String): Unit = {
    if (clusterInfoMap.isEmpty) {
      throw new IllegalArgumentException
    }
    val kafkaClusterSet = clusterInfoMap.keySet().asScala;
    kafkaClusterSet.foreach(eachCluster => {
      val topics = clusterInfoMap.get(eachCluster);
      val zkServers = eachCluster.getZkQuorum();
      val kafkaCluster = eachCluster.getKafkaCluster();
      val zkClient: ZkClient = new ZkClient(zkServers, ZK_TIMEOUT_MSEC, ZK_TIMEOUT_MSEC);
      try {
        val effectiveTopics = topics.asScala.filter(topic => AdminUtils.topicExists(zkClient, topic))
        logInfo("effectiveTopics" + effectiveTopics)
        val tp = kafkaCluster.getPartitions(effectiveTopics.toSet).right.get;
        val fromOffsets = new JHashMap[TopicAndPartition, JLong]();
        tp.foreach(
          eachTp => {
            val result = kafkaCluster.getConsumerOffsets(groupId, Set(eachTp))
            if (result.isLeft) {
              fromOffsets.put(eachTp, 0L)
            } else {
              result.right.get.foreach(tpAndOffset => {
                fromOffsets.put(tpAndOffset._1, tpAndOffset._2)
              })
            }
          }
        )
        logInfo("fillInLatestOffsets fromOffsets" + fromOffsets)
        fromOffsetsMap.put(eachCluster, fromOffsets)
      } finally {
        zkClient.close;
      }
    })
  }

  def refreshOffsets(topics: JSet[String], currentOffsets: JMap[TopicAndPartition, JLong], groupId: String, kafkaCluster: KafkaCluster, zkServers: String): JMap[TopicAndPartition, Long] = {
    if (topics == null) {
      //first init
      return null
    }
    val zkClient: ZkClient = new ZkClient(zkServers, ZK_TIMEOUT_MSEC, ZK_TIMEOUT_MSEC);
    var currentOffsetsWithOutNouseTopic: Map[TopicAndPartition, Long] = Map()
    try {
      val effectiveTopics = topics.asScala.filter(topic => AdminUtils.topicExists(zkClient, topic))
      val tp = kafkaCluster.getPartitions(effectiveTopics.toSet).right.get

      currentOffsets.asScala.keys.foreach(
        currentTp => {
          //remove nouse TopicAndPartition
          if (tp.contains(currentTp)) {
            val offset = currentOffsets.get(currentTp)
            currentOffsetsWithOutNouseTopic = currentOffsetsWithOutNouseTopic + (currentTp -> offset)
          }
        }
      )

      tp.foreach(
        eachTp => {
          if (!currentOffsetsWithOutNouseTopic.contains(eachTp)) {
            //add new TopicAndPartition
            val result = kafkaCluster.getConsumerOffsets(groupId, Set(eachTp))
            if (result.isLeft) {
              currentOffsetsWithOutNouseTopic = currentOffsetsWithOutNouseTopic + (eachTp -> 0L)
            } else {
              result.right.get.foreach(tpAndOffset => {
                currentOffsetsWithOutNouseTopic = currentOffsetsWithOutNouseTopic + (tpAndOffset._1 -> tpAndOffset._2)
              })
            }
          }
        }
      )

    } finally {
      zkClient.close;
    }
    currentOffsetsWithOutNouseTopic.asJava
  }

}
