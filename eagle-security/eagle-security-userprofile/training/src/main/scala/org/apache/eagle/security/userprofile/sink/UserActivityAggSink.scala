/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.eagle.security.userprofile.sink

import java.util.Properties

import kafka.producer.{KeyedMessage, Producer, ProducerConfig}
import org.apache.commons.math3.linear.RealMatrix
import org.apache.eagle.log.base.taggedlog.TaggedLogAPIEntity
import org.apache.eagle.security.userprofile.model.{EntityConversion, UserActivityAggModel}
import org.apache.spark.Logging
import org.apache.spark.rdd.RDD
import org.codehaus.jackson.map.ObjectMapper

/**
 * @since  0.3.0
 */
trait UserActivityAggRDDSink {
  def persist(rdd: RDD[(String, RealMatrix)],cmdTypes:Seq[String],site:String): Unit
}

abstract class UserActivityAggModelSink extends UserActivityAggRDDSink{
  override def persist(rdd: RDD[(String, RealMatrix)],cmdTypes:Seq[String],site:String): Unit = {
    rdd.foreach(kv => {
      this.persist(UserActivityAggModel(kv._1,kv._2,cmdTypes,site,System.currentTimeMillis()))
    })
  }

  def persist(model: UserActivityAggModel): Unit
}

case class UserActivityAggKafkaSink(properties: Properties) extends UserActivityAggModelSink with Logging{

  @transient var producer:Producer[String, String] = null
  @transient var objectMapper:ObjectMapper  = null

  val TOPIC_KEY = "topic"
  val topicName  = properties.getProperty(TOPIC_KEY)
  if(topicName == null) throw new IllegalArgumentException(s"$TOPIC_KEY is null")

  private[this] def getProducer = {
    if(producer == null){
      if(!properties.containsKey("serializer.class")) properties.setProperty("serializer.class", "kafka.serializer.StringEncoder")
      if(!properties.containsKey("metadata.broker.list")) properties.setProperty("metadata.broker.list", "localhost:9092")
      val config = new ProducerConfig(properties)
        producer = new Producer[String,String](config)
    }
    producer
  }

  private[this] def getObjectMapper = {
    if(objectMapper == null) {
      objectMapper = TaggedLogAPIEntity.buildObjectMapper()
    }
    objectMapper
  }

  override def persist(model: UserActivityAggModel): Unit = {
    val entity = model.asInstanceOf[EntityConversion[TaggedLogAPIEntity]].toEntity
    val message = new KeyedMessage[String, String](topicName, model.user, getObjectMapper.writeValueAsString(entity))
    try {
      getProducer.send(message)
    }catch {
      case e:Exception => {
        logError(s"Failed to send message to kafka[$properties]",e)
        throw e
      }
    }
  }
}