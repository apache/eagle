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

import backtype.storm.spout.SpoutOutputCollector
import backtype.storm.task.TopologyContext
import backtype.storm.topology.OutputFieldsDeclarer
import backtype.storm.topology.base.BaseRichSpout
import backtype.storm.tuple.Fields
import com.typesafe.config.Config
import org.slf4j.LoggerFactory

case class TestSpout() extends BaseRichSpout {
  val LOG = LoggerFactory.getLogger(TestSpout.getClass)
  var _collector : SpoutOutputCollector = null
  override def nextTuple : Unit = {
    _collector.emit(util.Arrays.asList("abc"))
    LOG.info("send spout data abc")
    Thread.sleep(1000)
  }
  override def declareOutputFields (declarer: OutputFieldsDeclarer): Unit ={
    declarer.declare(new Fields("value"))
  }
  override def open(conf: java.util.Map[_, _], context: TopologyContext, collector: SpoutOutputCollector): Unit ={
    _collector = collector
  }
}

case class TestKeyValueSpout() extends BaseRichSpout {
  val LOG = LoggerFactory.getLogger(TestSpout.getClass)
  var _collector : SpoutOutputCollector = null
  var count : Int = 0
  override def nextTuple : Unit = {
    if(count%3 == 0) {
      _collector.emit(util.Arrays.asList("abc", new Integer(1)))
    }else{
      _collector.emit(util.Arrays.asList("xyz", new Integer(1)))
    }
    count += 1;
    LOG.info("send spout data abc/xyz")
    Thread.sleep(1000)
  }
  override def declareOutputFields (declarer: OutputFieldsDeclarer): Unit ={
    declarer.declare(new Fields("word", "count"))
  }
  override def open(conf: java.util.Map[_, _], context: TopologyContext, collector: SpoutOutputCollector): Unit ={
    _collector = collector
  }
}

case class EchoExecutor() extends StormStreamExecutor1[String] {
  val LOG = LoggerFactory.getLogger(EchoExecutor.getClass)
  var config : Config = null
  override def prepareConfig(config : Config){this.config = config}
  override def init {}
  override def flatMap(input : Seq[AnyRef], outputCollector : Collector[Tuple1[String]]): Unit ={
    outputCollector.collect(Tuple1(input.head.asInstanceOf[String]))
    LOG.info("echo " + input.head)
  }
}

case class WordPrependExecutor(prefix : String) extends StormStreamExecutor1[String] {
  val LOG = LoggerFactory.getLogger(WordPrependExecutor.getClass)
  var config : Config = null
  override def prepareConfig(config : Config){this.config = config}
  override def init {}
  override def flatMap(input : Seq[AnyRef], outputCollector : Collector[Tuple1[String]]): Unit ={
    outputCollector.collect(Tuple1(prefix + "_" + input.head))
    LOG.info("preappend " + prefix + "_" + input.head)
  }
}

case class WordPrependForAlertExecutor(prefix : String) extends StormStreamExecutor2[String, util.SortedMap[Object, Object]] {
  val LOG = LoggerFactory.getLogger(WordPrependExecutor.getClass)
  var config : Config = null
  override def prepareConfig(config : Config){this.config = config}
  override def init {}
  override def flatMap(input : Seq[AnyRef], outputCollector : Collector[Tuple2[String, util.SortedMap[Object, Object]]]): Unit ={
    val value = new util.TreeMap[Object, Object]()
    value.put("word", prefix + "_" + input.head)
    outputCollector.collect(Tuple2("key1",value))
    LOG.info("preappend " + prefix + "_" + input.head)
  }
}

case class WordPrependForAlertExecutor2(prefix : String) extends StormStreamExecutor1[util.SortedMap[Object, Object]] {
  val LOG = LoggerFactory.getLogger(WordPrependExecutor.getClass)
  var config : Config = null
  override def prepareConfig(config : Config){this.config = config}
  override def init {}
  override def flatMap(input : Seq[AnyRef], outputCollector : Collector[Tuple1[util.SortedMap[Object, Object]]]): Unit ={
    val value = new util.TreeMap[Object, Object]()
    value.put("word", prefix + "_" + input.head)
    outputCollector.collect(Tuple1(value))
    LOG.info("preappend " + prefix + "_" + input.head)
  }
}

case class WordAppendExecutor(suffix : String) extends StormStreamExecutor1[String] {
  val LOG = LoggerFactory.getLogger(WordPrependExecutor.getClass)
  var config : Config = null
  override def prepareConfig(config : Config){this.config = config}
  override def init {}
  override def flatMap(input : Seq[AnyRef], outputCollector : Collector[Tuple1[String]]): Unit ={
    outputCollector.collect(Tuple1(input.head + "_" + suffix))
    LOG.info("append " + input.head + "_" + suffix)
  }
}

case class WordAppendForAlertExecutor(suffix : String) extends StormStreamExecutor2[String, util.SortedMap[Object, Object]] {
  val LOG = LoggerFactory.getLogger(WordPrependExecutor.getClass)
  var config : Config = null
  override def prepareConfig(config : Config){this.config = config}
  override def init {}
  override def flatMap(input : Seq[AnyRef], outputCollector : Collector[Tuple2[String, util.SortedMap[Object, Object]]]): Unit ={
    val value = new util.TreeMap[Object, Object]()
    value.put("word", input.head + "_" + suffix)
    outputCollector.collect(Tuple2("key1", value))
    LOG.info("append " + input.head + "_" + suffix)
  }
}

case class PatternAlertExecutor(pattern : String) extends StormStreamExecutor1[String] {
  val LOG = LoggerFactory.getLogger(PatternAlertExecutor.getClass)
  var config : Config = null
  override def prepareConfig(config : Config){this.config = config}
  override def init {}
  override def flatMap(input : Seq[AnyRef], outputCollector : Collector[Tuple1[String]]): Unit ={
    LOG.info("send out " + input.head)
    if(input.head.asInstanceOf[String].matches(pattern)){
      LOG.info("Alert hadppens for input " + input.head + " and for pattern " + pattern)
    }
  }
}

case class GroupedEchoExecutor() extends StormStreamExecutor1[String] {
  val LOG = LoggerFactory.getLogger(GroupedEchoExecutor.getClass)
  var config : Config = null
  override def prepareConfig(config : Config){this.config = config}
  override def init {}
  override def flatMap(input : Seq[AnyRef], outputCollector : Collector[Tuple1[String]]): Unit ={
    LOG.info("get " + input(0))
  }
}