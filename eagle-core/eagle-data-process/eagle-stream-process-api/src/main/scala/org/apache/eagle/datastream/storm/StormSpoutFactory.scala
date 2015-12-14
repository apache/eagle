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
package org.apache.eagle.datastream.storm

import java.util

import backtype.storm.topology.base.BaseRichSpout
import com.typesafe.config.Config
import org.apache.eagle.datastream.core.{IterableStreamProducer, StormSourceProducer, StreamProducer}
import org.apache.eagle.datastream.utils.NameConstants

object StormSpoutFactory {
  def createSpout(config: Config, from: StreamProducer[Any]): BaseRichSpout = {
    implicit val streamInfo = from.getInfo
    from match {
      case p@StormSourceProducer(source) =>
        if(p.outKeyed) throw new IllegalStateException(s"groupByKey after $p is not implemented yet")
        createProxySpout(config, p)
      case p@IterableStreamProducer(iterable,recycle) =>
        IterableStreamSpout(iterable,recycle)
      case _ =>
        throw new IllegalArgumentException(s"Cannot compile unknown $from to a Storm Spout")
    }
  }

  /**
   * @param config context configuration
   * @param sourceProducer source producer
   * @return
   */
  def createProxySpout(config: Config, sourceProducer: StormSourceProducer[Any]): BaseRichSpout = {
    val numFields = sourceProducer.numFields
    if (numFields <= 0) {
      sourceProducer.source
    } else {
      var i = 0
      val ret = new util.ArrayList[String]
      while (i < numFields) {
        ret.add(NameConstants.FIELD_PREFIX + i)
        i += 1
      }
      SpoutProxy(sourceProducer.source, ret)
    }
  }
}