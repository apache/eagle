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

/**
 * @since  9/29/15
 */
case class MapBoltWrapper[T,R](num: Int, fn: T => R) extends BaseRichBolt {
  val LOG = LoggerFactory.getLogger(FilterBoltWrapper.getClass)
  var _collector : OutputCollector = null

  override def declareOutputFields(declarer: OutputFieldsDeclarer): Unit = {
    var fields = new util.ArrayList[String]()
    var i : Int = 0;
    while(i < num){
      fields.add(OutputFieldNameConst.FIELD_PREFIX + i)
      i += 1
    }
    declarer.declare(new Fields(fields))
  }

  override def execute(input: Tuple): Unit = {
    val size = input.size()
    var values : AnyRef = null
    size match {
      case 1 => values = scala.Tuple1(input.getValue(0))
      case 2 => values = scala.Tuple2(input.getValue(0), input.getValue(1))
      case 3 => values = scala.Tuple3(input.getValue(0), input.getValue(1), input.getValue(2))
      case 4 => values = scala.Tuple4(input.getValue(0), input.getValue(1), input.getValue(2), input.getValue(3))
      case _ => throw new IllegalArgumentException
    }
    val output = fn(values.asInstanceOf[T])
    output match {
      case scala.Tuple1(a) => _collector.emit(input, util.Arrays.asList(a.asInstanceOf[AnyRef]))
      case scala.Tuple2(a, b) => _collector.emit(input, util.Arrays.asList(a.asInstanceOf[AnyRef], b.asInstanceOf[AnyRef]))
      case scala.Tuple3(a, b, c) => _collector.emit(input, util.Arrays.asList(a.asInstanceOf[AnyRef], b.asInstanceOf[AnyRef], c.asInstanceOf[AnyRef]))
      case scala.Tuple4(a, b, c, d) => _collector.emit(input, util.Arrays.asList(a.asInstanceOf[AnyRef], b.asInstanceOf[AnyRef], c.asInstanceOf[AnyRef], d.asInstanceOf[AnyRef]))
      case a => _collector.emit(input, util.Arrays.asList(a.asInstanceOf[AnyRef]))
    }
    _collector.ack(input)
  }

  override def prepare(stormConf: util.Map[_, _], context: TopologyContext, collector: OutputCollector): Unit = {
    _collector = collector
  }
}