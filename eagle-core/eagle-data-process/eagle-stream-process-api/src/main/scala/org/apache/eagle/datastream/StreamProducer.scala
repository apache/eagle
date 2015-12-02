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
import org.apache.eagle.alert.entity.AlertAPIEntity
import org.jgrapht.experimental.dag.DirectedAcyclicGraph
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

/**
 * StreamProducer is the base class for all other concrete StreamProducer
 * Stream Producer can be treated as logical processing element but physical
 * It defines high level type-safe API for user to organize data stream flow
 *
 * StreamProducer is independent of execution environment
 */
trait StreamProducer[+T]{
  var name: String = null
  var parallelism: Int = 1
  var graph: DirectedAcyclicGraph[StreamProducer[Any], StreamConnector[Any,Any]] = null
  var config: Config = null

  private def incrementAndGetId() = UniqueId.incrementAndGetId()

  def setGraph(graph: DirectedAcyclicGraph[StreamProducer[Any], StreamConnector[Any,Any]]): Unit = this.graph = graph
  def getGraph: DirectedAcyclicGraph[StreamProducer[Any], StreamConnector[Any,Any]] = graph
  def setConfig(config: Config) : Unit = this.config = config
  def getConfig: Config = config

  def filter(fn : T => Boolean): StreamProducer[T] ={
    val ret = FilterProducer[T](incrementAndGetId(), fn)
    hookupDAG(this, ret)
    ret
  }

  @deprecated("Field-based flat mapper")
  def flatMap[R](flatMapper : FlatMapper [R]) : StreamProducer[R] = {
    val ret = FlatMapProducer[T,R](incrementAndGetId(), flatMapper)
    hookupDAG(this, ret)
    ret
  }

  def map1[R](fn : T => R) : StreamProducer[R] = {
    val ret = MapProducer[T,R](incrementAndGetId(), 1, fn)
    hookupDAG(this, ret)
    ret
  }

  def map2[R](fn : T => R) : StreamProducer[R] = {
    val ret = MapProducer[T,R](incrementAndGetId(), 2, fn)
    hookupDAG(this, ret)
    ret
  }

  def map3[R](fn : T => R) : StreamProducer[R] = {
    val ret = MapProducer(incrementAndGetId(), 3, fn)
    hookupDAG(this, ret)
    ret
  }

  def map4[R](fn : T => R) : StreamProducer[R] = {
    val ret = MapProducer(incrementAndGetId(), 4, fn)
    hookupDAG(this, ret)
    ret
  }

  /**
   * starting from 0, groupby operator would be upon edge of the graph
   */
  def groupBy(fields : Int*) : StreamProducer[T] = {
    // validate each field index is greater or equal to 0
    fields.foreach(n => if(n<0) throw new IllegalArgumentException("field index should be always >= 0"))
    val ret = GroupByProducer[T](incrementAndGetId(), fields)
    hookupDAG(this, ret)
    ret
  }

  //groupBy java version, starting from 1
  def groupBy(fields : java.util.List[Integer]) : StreamProducer[T] = {
    // validate each field index is greater or equal to 0
    fields.foreach(n => if(n<0) throw new IllegalArgumentException("field index should be always >= 0"))
    val ret = GroupByProducer[T](incrementAndGetId(), fields.asScala.toSeq.asInstanceOf[Seq[Int]])
    hookupDAG(this, ret)
    ret
  }

  def streamUnion[T2,T3](others : Seq[StreamProducer[T2]]) : StreamProducer[T3] = {
    val ret = StreamUnionProducer[T,T2,T3](incrementAndGetId(), others)
    hookupDAG(this, ret)
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
    hookupDAG(this, ret)
    ret
  }

  def alertWithConsumer(upStreamName: String, alertExecutorId : String) ={
    alert(util.Arrays.asList(upStreamName), alertExecutorId, true)
  }

  def alertWithoutConsumer(upStreamName: String, alertExecutorId : String) ={
    alert(util.Arrays.asList(upStreamName), alertExecutorId, false)
  }

  def hookupDAG[T1,T2](current: StreamProducer[T1], next: StreamProducer[T2]) = {
    current.getGraph.addVertex(next)
    current.getGraph.addEdge(current, next, StreamConnector(current, next))
    passOnContext(current, next)
  }

  private def passOnContext[T1 ,T2](current: StreamProducer[T1], next: StreamProducer[T2]): Unit ={
    next.graph = current.graph
    next.config = current.config
  }

  /**
   * can be set by programatically or by configuration
   */
  def withParallelism(parallelism : Int) : StreamProducer[T] = {
    this.parallelism = parallelism
    this
  }

  def withName(name : String) : StreamProducer[T] = {
    this.name = name
    this
  }
}

case class FilterProducer[T](id: Int, fn : T => Boolean) extends StreamProducer[T]

case class FlatMapProducer[T, R](id: Int, var mapper: FlatMapper[R]) extends StreamProducer[R] {
  override def toString = mapper.toString + "_" + id
}

case class MapProducer[T,R](id: Int, numOutputFields : Int, var fn : T => R) extends StreamProducer[R]

case class GroupByProducer[T](id: Int, fields : Seq[Int]) extends StreamProducer[T]

case class StreamUnionProducer[T1,T2,T3](id: Int, others: Seq[StreamProducer[T2]]) extends StreamProducer[T3]

case class StormSourceProducer[+T](id: Int, source : BaseRichSpout) extends StreamProducer[T]{
  var numFields : Int = 0
  /**
    * rename outputfields to f0, f1, f2, ...
   * if one spout declare some field names, those fields names will be modified
   * @param n
   */
  def renameOutputFields(n : Int): StormSourceProducer[T] ={
    this.numFields = n
    this
  }
}

case class AlertStreamSink(id: Int, upStreamNames: util.List[String], alertExecutorId : String, var withConsumer: Boolean=true) extends StreamProducer[AlertAPIEntity]{
  def consume(withConsumer:Boolean): AlertStreamSink ={
    this.withConsumer = withConsumer
    this
  }
}

object UniqueId{
  val id : AtomicInteger = new AtomicInteger(0);
  def incrementAndGetId() : Int = {
    id.incrementAndGet()
  }
}