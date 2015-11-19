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
import java.util.concurrent.atomic.AtomicInteger

import backtype.storm.topology.base.BaseRichSpout
import com.typesafe.config.Config
import org.jgrapht.experimental.dag.DirectedAcyclicGraph
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

/**
 * StreamProducer is the base class for all other concrete StreamProducer
 * It defines high level API for user to organize data stream flow
 *
 * StreamProducer is independent of execution environment
 */

trait StreamProducer{
  var name: String = null
  var parallelism: Int = 1
  var graph: DirectedAcyclicGraph[StreamProducer, StreamConnector] = null
  var config: Config = null

  private def incrementAndGetId() = UniqueId.incrementAndGetId()

  def setGraph(graph: DirectedAcyclicGraph[StreamProducer, StreamConnector]): Unit = this.graph = graph
  def getGraph: DirectedAcyclicGraph[StreamProducer, StreamConnector] = graph
  def setConfig(config: Config) : Unit = this.config = config
  def getConfig: Config = config

  def filter(fn : AnyRef => Boolean): StreamProducer ={
    val ret = FilterProducer(incrementAndGetId(), fn)
    hookupDAG(graph, this, ret)
    ret
  }

  def flatMap[T, R](mapper : FlatMapper[T, R]) : StreamProducer = {
    val ret = FlatMapProducer(incrementAndGetId(), mapper)
    hookupDAG(graph, this, ret)
    ret
  }

  def map1(fn : AnyRef => AnyRef) : StreamProducer = {
    val ret = MapProducer(incrementAndGetId(), 1, fn)
    hookupDAG(graph, this, ret)
    ret
  }

  def map2(fn : AnyRef => AnyRef) : StreamProducer = {
    val ret = MapProducer(incrementAndGetId(), 2, fn)
    hookupDAG(graph, this, ret)
    ret
  }

  def map3(fn : AnyRef => AnyRef) : StreamProducer = {
    val ret = MapProducer(incrementAndGetId(), 3, fn)
    hookupDAG(graph, this, ret)
    ret
  }

  def map4(fn : AnyRef => AnyRef) : StreamProducer = {
    val ret = MapProducer(incrementAndGetId(), 4, fn)
    hookupDAG(graph, this, ret)
    ret
  }

  /**
   * starting from 0, groupby operator would be upon edge of the graph
   */
  def groupBy(fields : Int*) : StreamProducer = {
    // validate each field index is greater or equal to 0
    fields.foreach(n => if(n<0) throw new IllegalArgumentException("field index should be always >= 0"))
    val ret = GroupByProducer(incrementAndGetId(), fields)
    hookupDAG(graph, this, ret)
    ret
  }

  //groupBy java version, starting from 1
  def groupBy(fields : java.util.List[Integer]) : StreamProducer = {
    // validate each field index is greater or equal to 0
    fields.foreach(n => if(n<0) throw new IllegalArgumentException("field index should be always >= 0"))
    val ret = GroupByProducer(incrementAndGetId(), fields.asScala.toSeq.asInstanceOf[Seq[Int]])
    hookupDAG(graph, this, ret)
    ret
  }

  def streamUnion(others : Seq[StreamProducer]) : StreamProducer = {
    val ret = StreamUnionProducer(incrementAndGetId(), others)
    hookupDAG(graph, this, ret)
    ret
  }

  /**
   * alert is always sink of data flow
   */
  def alertWithConsumer(upStreamNames: util.List[String], alertExecutorId : String) = {
    alert(upStreamNames, alertExecutorId, true)
  }

  def alertWithoutConsumer(upStreamNames: util.List[String], alertExecutorId : String) = {
    alert(upStreamNames, alertExecutorId, false)
  }

  def alert(upStreamNames: util.List[String], alertExecutorId : String, withConsumer: Boolean=true) = {
    val ret = AlertStreamSink(incrementAndGetId(), upStreamNames, alertExecutorId, withConsumer)
    hookupDAG(graph, this, ret)
  }

  def alertWithConsumer(upStreamName: String, alertExecutorId : String): Unit ={
    alert(util.Arrays.asList(upStreamName), alertExecutorId, true)
  }

  def alertWithoutConsumer(upStreamName: String, alertExecutorId : String): Unit ={
    alert(util.Arrays.asList(upStreamName), alertExecutorId, false)
  }

  def hookupDAG(graph: DirectedAcyclicGraph[StreamProducer, StreamConnector], current: StreamProducer, next: StreamProducer) = {
    current.getGraph.addVertex(next)
    current.getGraph.addEdge(current, next, StreamConnector(current, next))
    passOnContext(current, next)
  }

  private def passOnContext(current: StreamProducer, next: StreamProducer): Unit ={
    next.graph = current.graph
    next.config = current.config
  }

  /**
   * can be set by programatically or by configuration
   */
  def withParallelism(parallelism : Int) : StreamProducer = {
    this.parallelism = parallelism
    this
  }

  def withName(name : String) : StreamProducer = {
    this.name = name
    this
  }
}

case class FilterProducer(id: Int, fn : AnyRef => Boolean) extends StreamProducer

case class FlatMapProducer[T, R](id: Int, var mapper: FlatMapper[T, R]) extends StreamProducer {
  override def toString() = mapper.toString + "_" + id
}

case class MapProducer(id: Int, numOutputFields : Int, var fn : AnyRef => AnyRef) extends StreamProducer

case class GroupByProducer(id: Int, fields : Seq[Int]) extends StreamProducer

case class StreamUnionProducer(id: Int, others: Seq[StreamProducer]) extends StreamProducer

case class StormSourceProducer(id: Int, source : BaseRichSpout) extends StreamProducer{
  var numFields : Int = 0
  /**
    * rename outputfields to f0, f1, f2, ...
   * if one spout declare some field names, those fields names will be modified
   * @param n
   */
  def renameOutputFields(n : Int): StormSourceProducer ={
    this.numFields = n
    this
  }
}

case class AlertStreamSink(id: Int, upStreamNames: util.List[String], alertExecutorId : String, withConsumer: Boolean=true) extends StreamProducer

object UniqueId{
  val id : AtomicInteger = new AtomicInteger(0);
  def incrementAndGetId() : Int = {
    id.incrementAndGet()
  }
}