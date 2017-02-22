/*
  * Licensed to the Apache Software Foundation (ASF) under one or more
  * contributor license agreements.  See the NOTICE file distributed with
  * this work for additional information regarding copyright ownership.
  * The ASF licenses this file to You under the Apache License, Version 2.0
  * (the "License"); you may not use this file except in compliance with
  * the License.  You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  */
package org.apache.eagle.log4j.kafka

import java.util.Properties

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.log4j.{Level, Logger}
import org.junit.Test

class KafkaLog4jAppenderIT extends KafkaTestBase {
  val KafkaLog4jAppenderTopic = "KafkaLog4jAppender"
  val JKafkaLog4jAppenderTopic = "JKafkaLog4jAppender"
  val kafkaBrokerList = "localhost:" + kafkaPort

  Logger.getRootLogger.setLevel(Level.ALL)

  val KafkaLog4jAppenderLogger = Logger.getLogger(classOf[KafkaLog4jAppender])
  val JKafkaLog4jAppenderLogger = Logger.getLogger(classOf[JKafkaLog4jAppender])

  def createConsumerConfig(): Properties = {
    val props = new Properties()
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBrokerList)
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "consumer")
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true")
    props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000")
    props.put(ConsumerConfig.SESSION_TIMEOUT_MS, "30000")
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
    props.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY, "kafka.consumer.RoundRobinAssignor")
    props
  }

  @Test def testKafkaLog4jAppender(): Unit = {
    val kafkaLog4jAppender = new KafkaLog4jAppender()
    try {
      kafkaLog4jAppender.setName(classOf[KafkaLog4jAppender].getName)
      kafkaLog4jAppender.setTopic(KafkaLog4jAppenderTopic)
      kafkaLog4jAppender.setBrokerList(kafkaBrokerList)
      kafkaLog4jAppender.setBatchSize(1)
      kafkaLog4jAppender.setQueueSize("1")

      kafkaLog4jAppender.activateOptions()
      KafkaLog4jAppenderLogger.addAppender(kafkaLog4jAppender)
      KafkaLog4jAppenderLogger.info("message to KafkaLog4jAppender")
    } finally {
      kafkaLog4jAppender.close()
    }
  }

  @Test def testJKafkaLog4jAppender(): Unit = {
    val jKafkaLog4jAppender = new JKafkaLog4jAppender()
    try {
      jKafkaLog4jAppender.setName(classOf[JKafkaLog4jAppender].getName)
      jKafkaLog4jAppender.setTopic(JKafkaLog4jAppenderTopic)
      jKafkaLog4jAppender.setBrokerList(kafkaBrokerList)
      jKafkaLog4jAppender.setSyncSend(true)
      jKafkaLog4jAppender.setRequiredNumAcks(0)
      jKafkaLog4jAppender.activateOptions()
      JKafkaLog4jAppenderLogger.addAppender(jKafkaLog4jAppender)
      JKafkaLog4jAppenderLogger.info("message to JKafkaLog4jAppender")
    } finally {
      jKafkaLog4jAppender.close()
    }
  }
}
