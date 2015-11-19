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
package org.apache.eagle.log4j.kafka.hadoop

import java.util.Properties
import org.apache.eagle.log4j.kafka.TopicPicker

/**
 * @since  6/26/15
 */
class SecurityLogTopicPicker extends TopicPicker{
  private var failedAuthnTopic: String = null
  private var securityTopic: String = null

  val FAILED: String = "failed"
  val TOPIC: String = "Topic"

  def this(props:Properties){
    this()
    val topic = props.getProperty(TOPIC)
    val topics = if(topic!=null) topic.split(",") else Array(topic)

    if(topics != null){
      if(topics.length > 0) failedAuthnTopic = topics(0)
      if(topics.length > 1) securityTopic = topics(1)
    }
  }

  /**
   * returns a topic based on the message.
   */
  override def getTopic(value: String): String = {
    val fields = value.split("\\s+")
    if(fields.length > 5 && fields(5).equals(FAILED))
      failedAuthnTopic
    else
      securityTopic
  }
}