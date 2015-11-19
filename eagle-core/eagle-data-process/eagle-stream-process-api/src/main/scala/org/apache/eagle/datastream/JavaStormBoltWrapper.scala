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
package org.apache.eagle.datastream

import java.util

import backtype.storm.task.{OutputCollector, TopologyContext}
import backtype.storm.topology.OutputFieldsDeclarer
import backtype.storm.topology.base.BaseRichBolt
import backtype.storm.tuple.{Fields, Tuple}
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._

case class JavaStormBoltWrapper(worker : JavaStormStreamExecutor[EagleTuple]) extends BaseRichBolt{
  val LOG = LoggerFactory.getLogger(StormBoltWrapper.getClass)
  var _collector : OutputCollector = null

  override def prepare(stormConf: util.Map[_, _], context: TopologyContext, collector: OutputCollector): Unit = {
    _collector = collector
    worker.init
  }

  override def execute(input : Tuple): Unit ={
    worker.flatMap(input.getValues, new Collector[EagleTuple](){
      def collect(t: EagleTuple): Unit ={
        _collector.emit(input, t.getList.asJava)
      }
    })
    _collector.ack(input)
  }

  override def declareOutputFields(declarer : OutputFieldsDeclarer): Unit ={
    val fields = worker.fields
    LOG.info("output fields for worker " + worker + " : " + fields.toList)
    declarer.declare(new Fields(fields:_*))
  }
}
