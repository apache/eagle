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
package org.apache.eagle.datastream.kafka

import java.io.IOException
import java.util
import java.util.Properties

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.eagle.dataproc.impl.storm.kafka.SpoutKafkaMessageDeserializer
import org.slf4j.{Logger, LoggerFactory}

/**
 * @since  11/6/15
 */
case class JsonMessageDeserializer(props:Properties) extends SpoutKafkaMessageDeserializer{
  private val objectMapper: ObjectMapper = new ObjectMapper
  private val LOG: Logger = LoggerFactory.getLogger(classOf[JsonMessageDeserializer])

  override def deserialize(bytes: Array[Byte]): AnyRef = {
    var map: util.Map[String, _] = null
    try {
      map = objectMapper.readValue(bytes, classOf[util.TreeMap[String, _]])
    } catch {
      case e: IOException => {
        LOG.error("Failed to deserialize json from: " + new String(bytes), e)
      }
    }
    map
  }
}