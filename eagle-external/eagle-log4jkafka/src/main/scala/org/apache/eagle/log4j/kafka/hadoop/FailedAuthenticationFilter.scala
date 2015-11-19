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

import org.apache.log4j.spi.{Filter, LoggingEvent}

/**
 * @since  6/26/15
 */
class FailedAuthenticationFilter extends Filter{

  override def decide(event: LoggingEvent): Int = {
    event.getMessage match {
      case message: String =>
        val fields: Array[String] = message.split("\\s+")
        if (fields.length > 5 && fields(3).equals(FailedAuthenticationFilter.AUTH_LOG_SOURCE) && fields(5).equals(FailedAuthenticationFilter.FAILED)) {
          Filter.DENY
        }
      case _ =>
    }
    Filter.NEUTRAL
  }
}

object FailedAuthenticationFilter{
  val AUTH_LOG_SOURCE: String = "SecurityLogger.org.apache.hadoop.ipc.Server:"
  val FAILED: String = "failed"
}