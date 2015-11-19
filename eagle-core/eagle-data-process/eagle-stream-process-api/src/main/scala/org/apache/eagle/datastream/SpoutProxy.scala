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

import backtype.storm.spout.SpoutOutputCollector
import backtype.storm.task.TopologyContext
import backtype.storm.topology.OutputFieldsDeclarer
import backtype.storm.topology.base.BaseRichSpout
import backtype.storm.tuple.Fields

/**
 * Declare delegated BaseRichSpout with given field names
 *
 * @param delegate delegated BaseRichSpout
 * @param outputFields given field names
 */
case class SpoutProxy(delegate: BaseRichSpout, outputFields: java.util.List[String]) extends BaseRichSpout{
  def open(conf: java.util.Map[_, _], context: TopologyContext, collector: SpoutOutputCollector) {
    this.delegate.open(conf, context, collector)
  }

  def nextTuple {
    this.delegate.nextTuple
  }

  override def ack(msgId: AnyRef) {
    this.delegate.ack(msgId)
  }

  override def fail(msgId: AnyRef) {
    this.delegate.fail(msgId)
  }

  override def deactivate {
    this.delegate.deactivate
  }

  override def declareOutputFields(declarer: OutputFieldsDeclarer) {
    declarer.declare(new Fields(outputFields))
  }

  override def close {
    this.delegate.close
  }
}
