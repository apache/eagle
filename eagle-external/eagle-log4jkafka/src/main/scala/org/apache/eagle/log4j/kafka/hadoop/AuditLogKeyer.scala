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
import org.apache.eagle.log4j.kafka.Keyer

class AuditLogKeyer(props:Properties) extends Keyer{
  /**
   * Uses the key to calculate a partition bucket id for routing
   * the data to the appropriate broker partition
   * @return returns a key based on the message
   */
  override def getKey(msg: String): String = if(msg != null) AuditLogKeyer.getKey(msg.split("\\s+")) else null
}

object AuditLogKeyer{
  def getKey(fields: Array[String]):String = if(fields.length > 5) AuditLogUtils.parseUser(fields(5)) else null
}