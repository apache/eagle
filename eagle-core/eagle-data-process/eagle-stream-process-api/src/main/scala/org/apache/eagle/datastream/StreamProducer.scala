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
import scala.collection.JavaConversions
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

/**
 * Stream interaction protocol interface
 *
 * @tparam T processed elements type
 */
trait StreamProtocol[+T]{
  def flatMap[R](flatMapper : FlatMapper [R]) : StreamProducer[R]

  def filter(fn : T => Boolean): StreamProducer[T]

  def map1[R](fn : T => R) : StreamProducer[R]
  def map2[R](fn : T => R) : StreamProducer[R]
  def map3[R](fn : T => R) : StreamProducer[R]
  def map4[R](fn : T => R) : StreamProducer[R]

  def groupBy(fields : Int*) : StreamProducer[T]

  def groupBy(fields : java.util.List[Integer]) : StreamProducer[T]

  def union[T2,T3](others : Seq[StreamProducer[T2]]) : StreamProducer[T3]

  def alert(upStreamNames: Seq[String], alertExecutorId : String, consume: Boolean)

  /**
   * Set processing element parallelism setting
   * @param parallelism parallelism value
   * @return
   */
  def parallelism(parallelism : Int) : StreamProducer[T]

  /**
   * Set component name
   *
   * @param name
   * @return
   */
  def name(name : String) : StreamProducer[T]

  /**
   * Set stream name
   * @param stream stream name in String
   * @return
   */
  def stream(stream: String): StreamProducer[T]
}

/**
 *
 * Stream Producer = Process Element Metadata + Stream Protocol
 *
 * StreamProducer is processing logic element, used the base class for all other concrete StreamProducer
 * Stream Producer can be treated as logical processing element but physical
 * It defines high level type-safe API for user to organize data stream flow
 *
 * StreamProducer is independent of execution environment
 *
 * @param id process element id
 * @param stream stream id
 * @tparam T processed elements type
 */
abstract class StreamProducer[+T](val id:Int = UniqueId.incrementAndGetId(),var stream:String=null) extends StreamProtocol[T]{
  /**
   * Component name
   */
  var name: String = null
  var parallelism: Int = 1
  var graph: DirectedAcyclicGraph[StreamProducer[Any], StreamConnector[Any,Any]] = null
  var config: Config = null

  def setGraph(graph: DirectedAcyclicGraph[StreamProducer[Any], StreamConnector[Any,Any]]): Unit = this.graph = graph
  def getGraph: DirectedAcyclicGraph[StreamProducer[Any], StreamConnector[Any,Any]] = graph
  def setConfig(config: Config) : Unit = this.config = config
  def getConfig: Config = config

  override def stream(streamId:String):StreamProducer[T] = {
    this.stream = streamId;
    this
  }

  override def filter(fn : T => Boolean): StreamProducer[T] ={
    val ret = FilterProducer[T](fn)
    hookupDAG(this, ret)
    ret
  }

  @deprecated("Field-based flat mapper")
  override def flatMap[R](flatMapper : FlatMapper [R]) : StreamProducer[R] = {
    val ret = FlatMapProducer[T,R](flatMapper)
    hookupDAG(this, ret)
    ret
  }

  override def map1[R](fn : T => R) : StreamProducer[R] = {
    val ret = MapProducer[T,R](1, fn)
    hookupDAG(this, ret)
    ret
  }

  override def map2[R](fn : T => R) : StreamProducer[R] = {
    val ret = MapProducer[T,R](2, fn)
    hookupDAG(this, ret)
    ret
  }

  override def map3[R](fn : T => R) : StreamProducer[R] = {
    val ret = MapProducer(3, fn)
    hookupDAG(this, ret)
    ret
  }

  override def map4[R](fn : T => R) : StreamProducer[R] = {
    val ret = MapProducer(4, fn)
    hookupDAG(this, ret)
    ret
  }

  /**
   * starting from 0, groupby operator would be upon edge of the graph
   */
  override def groupBy(fields : Int*) : StreamProducer[T] = {
    // validate each field index is greater or equal to 0
    fields.foreach(n => if(n<0) throw new IllegalArgumentException("field index should be always >= 0"))
    val ret = GroupByProducer[T](fields)
    hookupDAG(this, ret)
    ret
  }

  //groupBy java version, starting from 1
  override def groupBy(fields : java.util.List[Integer]) : StreamProducer[T] = {
    // validate each field index is greater or equal to 0
    fields.foreach(n => if(n<0) throw new IllegalArgumentException("field index should be always >= 0"))
    val ret = GroupByProducer[T](fields.asScala.toSeq.asInstanceOf[Seq[Int]])
    hookupDAG(this, ret)
    ret
  }

  override def union[T2,T3](others : Seq[StreamProducer[T2]]) : StreamProducer[T3] = {
    val ret = StreamUnionProducer[T,T2,T3](others)
    hookupDAG(this, ret)
    ret
  }

  /**
   * alert is always sink of data flow
   */
  def alertWithConsumer(upStreamNames: util.List[String], alertExecutorId : String) = {
    alert(upStreamNames.asScala, alertExecutorId, consume = true)
  }

  def alertWithoutConsumer(upStreamNames: util.List[String], alertExecutorId : String) = {
    alert(upStreamNames.asScala, alertExecutorId, consume = false)
  }

  override def alert(upStreamNames: Seq[String], alertExecutorId : String, consume: Boolean = true) = {
    val ret = AlertStreamSink(JavaConversions.asJavaList(upStreamNames), alertExecutorId, consume)
    hookupDAG(this, ret)
    ret
  }

  def alertWithConsumer(upStreamName: String, alertExecutorId : String) ={
    alert(Seq(upStreamName), alertExecutorId, consume = true)
  }

  def alertWithoutConsumer(upStreamName: String, alertExecutorId : String) ={
    alert(Seq(upStreamName), alertExecutorId, consume = false)
  }

  protected def hookupDAG[T1,T2](current: StreamProducer[T1], next: StreamProducer[T2]) = {
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
  override def parallelism(parallelism : Int) : StreamProducer[T] = {
    this.parallelism = parallelism
    this
  }

  override def name(name : String) : StreamProducer[T] = {
    this.name = name
    this
  }
}

case class FilterProducer[T](fn : T => Boolean) extends StreamProducer[T]

case class FlatMapProducer[T, R](var mapper: FlatMapper[R]) extends StreamProducer[R] {
  override def toString = mapper.toString + "_" + id
}

case class MapProducer[T,R](numOutputFields : Int, var fn : T => R) extends StreamProducer[R]

case class GroupByProducer[T](fields : Seq[Int]) extends StreamProducer[T]

case class StreamUnionProducer[T1,T2,T3](others: Seq[StreamProducer[T2]]) extends StreamProducer[T3]

case class StormSourceProducer[+T](source : BaseRichSpout) extends StreamProducer[T]{
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

case class AlertStreamSink(upStreamNames: util.List[String], alertExecutorId : String, var withConsumer: Boolean=true) extends StreamProducer[AlertAPIEntity]{
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