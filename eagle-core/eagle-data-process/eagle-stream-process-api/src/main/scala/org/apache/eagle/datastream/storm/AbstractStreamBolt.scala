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
package org.apache.eagle.datastream.storm

import java.util

import backtype.storm.task.{OutputCollector, TopologyContext}
import backtype.storm.topology.OutputFieldsDeclarer
import backtype.storm.topology.base.BaseRichBolt
import backtype.storm.tuple.{Fields, Tuple}
import org.apache.eagle.datastream.{NameConstant, StreamInfo}

/**
 *
 * @param fieldsNum zero-fieldsNum may means something different
 * @param ack
 * @param streamInfo
 * @tparam T
 */
abstract class AbstractStreamBolt[T](val fieldsNum:Int=0, val ack:Boolean = true)(implicit streamInfo:StreamInfo) extends BaseRichBolt{
  private var _collector: OutputCollector = null

  /**
   * If outKeyed then
   *  Fields = ("key","value"]
   * elsif num > 0
   *  Fields = ["f0","f1",..,"fn"]
   * elsif num == 0
   *  Fields = ["f0"]
   * end
   *
   * @param declarer
   */
  override def declareOutputFields(declarer: OutputFieldsDeclarer): Unit = {
    if(streamInfo.outKeyed) {
      declarer.declare(new Fields(NameConstant.FIELD_KEY,NameConstant.FIELD_VALUE))
    }else{
      if(fieldsNum > 0) {
        val fields = new util.ArrayList[String]()
        var i: Int = 0
        while (i < fieldsNum) {
          fields.add(NameConstant.FIELD_PREFIX + i)
          i += 1
        }
        declarer.declare(new Fields(fields))
      }else if(fieldsNum == 0){
        declarer.declare(new Fields(NameConstant.FIELD_PREFIX + 0))
      }
    }
  }

  def emit(values:util.List[AnyRef])(implicit input:Tuple){
    if(streamInfo.outKeyed) {
      _collector.emit(input, util.Arrays.asList(streamInfo.keySelector.key(values).asInstanceOf[AnyRef],values))
    }else {
      _collector.emit(input,values)
    }
  }

  def emit(value:Any)(implicit input:Tuple){
    if(streamInfo.outKeyed) {
      _collector.emit(input, util.Arrays.asList(streamInfo.keySelector.key(value).asInstanceOf[AnyRef],value.asInstanceOf[AnyRef]))
    }else{
      _collector.emit(input,util.Arrays.asList(value.asInstanceOf[AnyRef]))
    }
  }

  override def execute(input: Tuple): Unit = {
    implicit val _input = input
    if(streamInfo.inKeyed){
      val key = input.getValueByField(NameConstant.FIELD_KEY)
      val value = input.getValueByField(NameConstant.FIELD_VALUE).asInstanceOf[T]
      handleKeyValue(key,value)
    }else{
      handleValues(input.getValues)
    }
    if(ack) _collector.ack(input)
  }

  /**
   * Handle keyed stream value
   */
  def handleKeyValue(key:Any,value:T)(implicit input:Tuple)

  /**
   * Handle general stream values list
   *
   * @param values
   */
  def handleValues(values:util.List[AnyRef])(implicit input:Tuple)

  override def prepare(stormConf: util.Map[_, _], context: TopologyContext, collector: OutputCollector): Unit = {
    _collector = collector
  }
}