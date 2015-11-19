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
package org.apache.eagle.datastream

import java.util

import backtype.storm.topology.base.BaseRichSpout
import com.typesafe.config.Config

object StormSpoutFactory {
  /**
   * @param config context configuration
   * @param sourceProducer source producer
   * @return
   */
  def createSpout(config: Config, sourceProducer: StormSourceProducer) : BaseRichSpout = {
    val numFields = sourceProducer.numFields
    if(numFields <= 0) {
      sourceProducer.source
    }else{
      var i = 0
      val ret = new util.ArrayList[String]
      while(i < numFields){
        ret.add(OutputFieldNameConst.FIELD_PREFIX + i)
        i += 1
      }
      SpoutProxy(sourceProducer.source, ret)
    }
  }
}
