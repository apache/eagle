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
package org.apache.eagle.datastream.core

import java.util
import java.util.concurrent.atomic.AtomicInteger

import backtype.storm.topology.base.BaseRichSpout
import com.typesafe.config.Config
import org.apache.eagle.alert.entity.AlertAPIEntity
import org.apache.eagle.datastream._
import org.apache.eagle.partition.PartitionStrategy
import org.jgrapht.experimental.dag.DirectedAcyclicGraph

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.reflect.runtime.{universe => ru}

/**
 * StreamProducer = StreamInfo + StreamProtocol
 *
 * StreamProducer is processing logic element, used the base class for all other concrete StreamProducer
 * Stream Producer can be treated as logical processing element but physical
 * It defines high level type-safe API for user to organize data stream flow
 *
 * StreamProducer is independent of execution environment
 *
 * @tparam T processed elements type
 */
abstract class StreamProducer[+T <: Any] extends StreamInfo with StreamProtocol[T]{
  /**
   * Component name
   */
  var graph: DirectedAcyclicGraph[StreamProducer[Any], StreamConnector[Any,Any]] = null
  var config: Config = null

  /**
   * Should not modify graph when setting it
   *
   */
  def setGraph(graph: DirectedAcyclicGraph[StreamProducer[Any], StreamConnector[Any,Any]]): Unit = {
    this.graph = graph
  }

  def getGraph: DirectedAcyclicGraph[StreamProducer[Any], StreamConnector[Any,Any]] = graph
  def setConfig(config: Config) : Unit = this.config = config
  def getConfig: Config = config

  /**
   * @param graph
   * @param config
   * @return
   */
  override def init[E:ru.TypeTag](graph:DirectedAcyclicGraph[StreamProducer[Any], StreamConnector[Any,Any]],config:Config):Unit ={
    this.typeTag = ru.typeTag[E].asInstanceOf[ru.TypeTag[T]]
    this.typeClass=this.typeTag.mirror.runtimeClass(this.typeTag.tpe).asInstanceOf[Class[T]]
    this.setConfig(config)
    this.setGraph(graph)
    if(!this.graph.containsVertex(this)){
      this.graph.addVertex(this)
    }
  }

  /**
   * Get stream pure metadata info
   * @return
   */
  override def getInfo:StreamInfo = this.asInstanceOf[StreamInfo]

  override def stream(streamId:String):StreamProducer[T] = {
    this.streamId = streamId
    this
  }

  override def filter(fn : T => Boolean): StreamProducer[T] ={
    val ret = FilterProducer[T](fn)
    hookup(this, ret)(typeTag)
    ret
  }

  @deprecated("Field-based flat mapper")
  override def flatMap[R](flatMapper : FlatMapper [R])(implicit tag:ru.TypeTag[R]): StreamProducer[R] = {
    val ret = FlatMapProducer[T,R](flatMapper)
    hookup(this, ret)(tag)
    ret
  }

  override def flatMap[R](flatMapper: JFlatMapper[R]): StreamProducer[R] = {
    val ret = FlatMapProducer[T,R](flatMapper)
    hookup(this, ret)(ru.typeTag[AnyRef])
    ret
  }

  override def foreach(fn : T => Unit) : Unit = {
    val ret = ForeachProducer[T](fn)
    hookup(this, ret)(ru.typeTag[Unit])
  }

  override def map[R:ru.TypeTag](fn : T => R) : StreamProducer[R] = {
    val ret = MapperProducer[T,R](0,fn)
    hookup(this, ret)(ru.typeTag[R])
    ret
  }

  override def map1[R:ru.TypeTag](fn : T => R): StreamProducer[R] = {
    val ret = MapperProducer[T,R](1, fn)
    hookup(this, ret)(ru.typeTag[R])
    ret
  }

  override def map2[R:ru.TypeTag](fn : T => R): StreamProducer[R] = {
    val ret = MapperProducer[T,R](2, fn)
    hookup(this, ret)(ru.typeTag[R])
    ret
  }

  override def map3[R:ru.TypeTag](fn : T => R): StreamProducer[R] = {
    val ret = MapperProducer(3, fn)
    hookup(this, ret)(ru.typeTag[R])
    ret
  }

  override def map4[R:ru.TypeTag](fn : T => R): StreamProducer[R] = {
    val ret = MapperProducer(4, fn)
    hookup(this, ret)(ru.typeTag[R])
    ret
  }

  /**
   * starting from 0, groupby operator would be upon edge of the graph
   */
  override def groupBy(fields : Int*) : StreamProducer[T] = {
    // validate each field index is greater or equal to 0
    fields.foreach(n => if(n<0) throw new IllegalArgumentException("field index should be always >= 0"))
    val ret = GroupByFieldProducer[T](fields)
    hookup(this, ret)(typeTag)
    ret
  }

  //groupBy java version, starting from 1
  override def groupBy(fields : java.util.List[Integer]) : StreamProducer[T] = {
    // validate each field index is greater or equal to 0
    fields.foreach(n => if(n<0) throw new IllegalArgumentException("field index should be always >= 0"))
    val ret = GroupByFieldProducer[T](fields.asScala.toSeq.asInstanceOf[Seq[Int]])
    hookup(this, ret)(typeTag)
    ret
  }

  override def groupByKey(keySelector: T=> Any):StreamProducer[T] = {
    val ret = GroupByKeyProducer(keySelector)
    hookup(this,ret)(typeTag)
    ret
  }

  override def union[T2,T3](others : Seq[StreamProducer[T2]]) : StreamProducer[T3] = {
    val ret = StreamUnionProducer[T, T2, T3](others)
    hookup(this, ret)(typeTag)
    ret
  }

  def union[T2,T3](others : util.List[StreamProducer[T2]]) : StreamProducer[T3] = union(others)

  override def groupBy(strategy : PartitionStrategy) : StreamProducer[T] = {
    val ret = GroupByStrategyProducer(strategy)
    hookup(this, ret)(typeTag)
    ret
  }

  /**
   * alert is always sink of data flow
   */
  def alertWithConsumer(upStreamNames: util.List[String], alertExecutorId : String) = {
    alert(upStreamNames.asScala, alertExecutorId,consume = true)
  }

  def alertWithoutConsumer(upStreamNames: util.List[String], alertExecutorId : String) = {
    alert(upStreamNames.asScala, alertExecutorId, consume = false)
  }

  def alert(upStreamNames: Seq[String], alertExecutorId : String, consume: Boolean=true, strategy : PartitionStrategy=null) = {
    val ret = AlertStreamSink(upStreamNames, alertExecutorId, consume, strategy)
    hookup(this, ret)(ru.typeTag[AlertAPIEntity])
  }

  def alertWithConsumer(upStreamName: String, alertExecutorId : String, strategy: PartitionStrategy): Unit ={
    alert(util.Arrays.asList(upStreamName), alertExecutorId, consume = true, strategy = strategy)
  }

  def alertWithConsumer(upStreamName: String, alertExecutorId : String): Unit ={
    alert(util.Arrays.asList(upStreamName), alertExecutorId, consume = true)
  }

  def alertWithoutConsumer(upStreamName: String, alertExecutorId : String, strategy: PartitionStrategy): Unit ={
    alert(util.Arrays.asList(upStreamName), alertExecutorId, consume = false, strategy)
  }

  def alertWithoutConsumer(upStreamName: String, alertExecutorId : String): Unit ={
    alert(util.Arrays.asList(upStreamName), alertExecutorId, consume = false)
  }

  protected def hookup[T1,T2](current: StreamProducer[T1], next: StreamProducer[T2])(implicit nextTypeTag:ru.TypeTag[_]) = {
    current.getGraph.addVertex(next)
    current.getGraph.addEdge(current, next, StreamConnector(current, next))
    passOnContext[T1,T2](current, next)(if(nextTypeTag == null) ru.typeTag[AnyRef] else nextTypeTag)
  }

  private def passOnContext[T1 ,T2](current: StreamProducer[T1], next: StreamProducer[T2])(implicit nextTypeTag:ru.TypeTag[_]): Unit ={
    next.init(current.graph,current.config)
  }

  /**
   * can be set by programatically or by configuration
   */
  override def parallelism(parallelism : Int) : StreamProducer[T] = {
    this.parallelismNum = parallelism
    this
  }

  override def parallelism : Int = this.parallelismNum
  override def stream:String = this.streamId

  /**
   * Component name
   * 
   * @param componentName component name
   * @return
   */
  override def nameAs(componentName : String) : StreamProducer[T] = {
    this.name = componentName
    this
  }
}

case class FilterProducer[T](fn : T => Boolean) extends StreamProducer[T]{
  override def toString: String = s"Filter[${typeClass.getSimpleName}]"
}

case class FlatMapProducer[T, R](var mapper: FlatMapper[R]) extends StreamProducer[R]{
  override def toString: String = s"FlatMap[${typeClass.getSimpleName}]"
}

case class MapperProducer[T,R](numOutputFields : Int, var fn : T => R) extends StreamProducer[R]{
  override def toString: String = s"Map[${typeClass.getSimpleName}]"
}

case class ForeachProducer[T](var fn : T => Unit) extends StreamProducer[T]{
  override def toString: String = s"Foreach[${typeClass.getSimpleName}]"
}

abstract class GroupByProducer[T] extends StreamProducer[T]
case class GroupByFieldProducer[T](fields : Seq[Int]) extends GroupByProducer[T]
case class GroupByStrategyProducer[T](partitionStrategy: PartitionStrategy) extends GroupByProducer[T]
case class GroupByKeyProducer[T](keySelectorFunc:T => Any) extends GroupByProducer[T]{
  override def toString: String = s"GroupByKey[$keySelectorFunc(${typeClass.getSimpleName})]"
}

object GroupByProducer {
  def apply[T](fields: Seq[Int]) = new GroupByFieldProducer[T](fields)
  def apply[T](partitionStrategy : PartitionStrategy) = new GroupByStrategyProducer[T](partitionStrategy)
  def apply[T](keySelector:T => Any) = new GroupByKeyProducer[T](keySelector)
}

case class StreamUnionProducer[T1,T2,T3](others: Seq[StreamProducer[T2]]) extends StreamProducer[T3]

case class StormSourceProducer[T](source : BaseRichSpout,var numFields : Int = 0) extends StreamProducer[T]{
  /**
    * rename outputfields to f0, f1, f2, ...
   * if one spout declare some field names, those fields names will be modified
   * @param n
   */
  def withOutputFields(n : Int): StormSourceProducer[T] ={
    this.numFields = n
    this
  }
}

case class IterableStreamProducer[T](iterable: Iterable[T],recycle:Boolean = false) extends StreamProducer[T]{
  override def toString: String = s"IterableStream[${typeClass.getSimpleName}]"
}

case class AlertStreamSink(upStreamNames: util.List[String], alertExecutorId : String, var consume: Boolean=true, strategy: PartitionStrategy=null) extends StreamProducer[AlertAPIEntity] {
  def consume(consume: Boolean): AlertStreamSink = {
    this.consume = consume
    this
  }
}

object UniqueId{
  val id : AtomicInteger = new AtomicInteger(0);
  def incrementAndGetId() : Int = {
    id.incrementAndGet()
  }
}

trait KeySelector extends Serializable{
  def key(t:Any):Any
}

case class KeySelectorWrapper[T](fn:T => Any) extends KeySelector{
  override def key(t: Any): Any = fn(t.asInstanceOf[T])
}