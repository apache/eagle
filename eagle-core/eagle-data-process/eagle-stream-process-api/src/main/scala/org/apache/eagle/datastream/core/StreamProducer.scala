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
package org.apache.eagle.datastream.core

import java.util
import java.util.concurrent.atomic.AtomicInteger

import backtype.storm.topology.base.BaseRichSpout
import com.typesafe.config.Config
import org.apache.eagle.alert.entity.AlertAPIEntity
import org.apache.eagle.datastream.{FlatMapperWrapperForSpark, FlatMapperWrapper, Collector, FlatMapper}
import org.apache.eagle.partition.PartitionStrategy
import org.jgrapht.experimental.dag.DirectedAcyclicGraph
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions.{asScalaBuffer, seqAsJavaList}
import scala.collection.JavaConverters.asScalaBufferConverter
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
abstract class StreamProducer[+T <: Any] extends StreamInfo with StreamProtocol[T] {
  private var graph: DirectedAcyclicGraph[StreamProducer[Any], StreamConnector[Any,Any]] = null
  private val LOG = LoggerFactory.getLogger(classOf[StreamProducer[T]])

  /**
   * Should not modify graph when setting it
   */
  def initWith(graph:DirectedAcyclicGraph[StreamProducer[Any], StreamConnector[Any,Any]],config:Config, hook:Boolean = true):StreamProducer[T]={
    this.config = config
    this.graph = graph
    if(hook && ! this.graph.containsVertex(this)) {
      this.graph.addVertex(this)
    }
    LOG.debug(this.graph.toString)
    this
  }
  
  /**
   * Get stream pure metadata info
    *
    * @return
   */
  override def getInfo:StreamInfo = this.asInstanceOf[StreamInfo]

  override def stream(streamId:String):StreamProducer[T] = {
    this.streamId = streamId
    this
  }

  override def filter(fn : T => Boolean): StreamProducer[T] ={
    val ret = FilterProducer[T](fn)
    connect(this, ret)
    ret
  }

  override def flatMap[R](flatMapper:FlatMapper[R]): StreamProducer[R] = {
    val ret = FlatMapProducer[T,R](flatMapper)
    connect(this, ret)
    ret
  }
  override def flatMap[R](func:(Any,Collector[R])=>Unit): StreamProducer[R] = {
    val ret = FlatMapProducer[T,R](FlatMapperWrapper[R](func))
    connect(this, ret)
    ret
  }

  def flatMap[R](func:T => Traversable[R]): StreamProducer[R] = {
    val ret = FlatMapProducer[T,R](FlatMapperWrapperForSpark[T,R](func))
    connect(this, ret)
    ret
  }

  def reduceByKey[V](fn: (V,V) => V) : StreamProducer[T] = {
    val ret = ReduceByKeyProducer[T,V](fn)
    connect(this, ret)
    ret
  }

  override def foreach(fn : T => Unit) : Unit = {
    val ret = ForeachProducer[T](fn)
    connect(this, ret)
  }

  override def map[R](fn : T => R) : StreamProducer[R] = {
    val ret = MapperProducer[T,R](0,fn)
    connect(this, ret)
    ret
  }

  override def map1[R](fn : T => R): StreamProducer[R] = {
    val ret = MapperProducer[T,R](1, fn)
    connect(this, ret)
    ret
  }

  override def map2[R](fn : T => R): StreamProducer[R] = {
    val ret = MapperProducer[T,R](2, fn)
    connect(this, ret)
    ret
  }

  override def map3[R](fn : T => R): StreamProducer[R] = {
    val ret = MapperProducer(3, fn)
    connect(this, ret)
    ret
  }

  override def map4[R](fn : T => R): StreamProducer[R] = {
    val ret = MapperProducer(4, fn)
    connect(this, ret)
    ret
  }

  /**
   * starting from 0, groupby operator would be upon edge of the graph
   */
  override def groupBy(fields : Int*) : StreamProducer[T] = {
    // validate each field index is greater or equal to 0
    fields.foreach(n => if(n<0) throw new IllegalArgumentException("field index should be always >= 0"))
    val ret = GroupByFieldProducer[T](fields)
    connect(this, ret)
    ret
  }

  def groupByFieldIndex(fields : Seq[Int]) : StreamProducer[T] = {
    // validate each field index is greater or equal to 0
    fields.foreach(n => if(n<0) throw new IllegalArgumentException("field index should be always >= 0"))
    val ret = GroupByFieldProducer[T](fields)
    connect(this, ret)
    ret
  }

  //groupBy java version, starting from 1
  override def groupBy(fields : java.util.List[Integer]) : StreamProducer[T] = {
    // validate each field index is greater or equal to 0
    fields.foreach(n => if(n<0) throw new IllegalArgumentException("field index should be always >= 0"))
    val ret = GroupByFieldProducer[T](fields.asScala.toSeq.asInstanceOf[Seq[Int]])
    connect(this, ret)
    ret
  }

  override def groupByKey(keySelector: T=> Any):StreamProducer[T] = {
    val ret = GroupByKeyProducer(keySelector)
    connect(this,ret)
    ret
  }

  override def streamUnion[T2,T3](others : Seq[StreamProducer[T2]]) : StreamProducer[T3] = {
    val ret = StreamUnionProducer[T, T2, T3](others)
    connect(this, ret)
    ret
  }

  def streamUnion[T2,T3](other : StreamProducer[T2]) : StreamProducer[T3] = {
    streamUnion(Seq(other))
  }

  def streamUnion[T2,T3](others : util.List[StreamProducer[T2]]) : StreamProducer[T3] = streamUnion(others.asScala)

  override def groupBy(strategy : PartitionStrategy) : StreamProducer[T] = {
    val ret = GroupByStrategyProducer(strategy)
    connect(this, ret)
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

  override def alert(upStreamNames: Seq[String], alertExecutorId : String, consume: Boolean=true, strategy : PartitionStrategy=null):AlertStreamProducer = {
    val ret = AlertStreamProducer(upStreamNames, alertExecutorId, consume, strategy)
    connect(this, ret)
    ret
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

  def aggregate(upStreamNames: java.util.List[String], queryExecutorId : String, strategy: PartitionStrategy = null): StreamProducer[T] = {
    val ret= AggregateProducer(upStreamNames, queryExecutorId, null, strategy)
    connect(this, ret)
    ret
  }

  def aggregateDirect(upStreamNames: java.util.List[String], cql : String, strategy: PartitionStrategy): StreamProducer[T] = {
    val ret= AggregateProducer(upStreamNames, null, cql, strategy)
    connect(this, ret)
    ret
  }

  def persist(executorId : String, storageType: StorageType.StorageType) : StreamProducer[T] = {
    val ret = PersistProducer(executorId, storageType)
    connect(this, ret)
    ret
  }

  def connect[T1,T2](current: StreamProducer[T1], next: StreamProducer[T2]) = {
    if(current.graph == null) throw new NullPointerException(s"$current has not been registered to any graph before being connected")
    current.graph.addVertex(next)
    current.graph.addEdge(current, next, StreamConnector(current, next))
    passOnContext[T1,T2](current, next)
  }

  def connect[T2]( next: StreamProducer[T2]) = {
    if(this.graph == null) throw new NullPointerException("graph is null")
    this.graph.addVertex(next)
    this.graph.addEdge(this, next, StreamConnector(this, next))
    passOnContext[T,T2](this, next)
  }

  private def passOnContext[T1 ,T2](current: StreamProducer[T1], next: StreamProducer[T2]): Unit ={
    next.initWith(current.graph,current.config)
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
  override def toString: String = s"FilterProducer"
}

case class FlatMapProducer[T, R](var mapper: FlatMapper[R]) extends StreamProducer[R]{
  override def toString: String = mapper.toString
}

case class ReduceByKeyProducer[T,V](fn: (V,V) => V) extends StreamProducer[T]{
  override def toString: String = s"ReduceByKeyProducer"
}

case class MapperProducer[T,R](numOutputFields : Int, var fn : T => R) extends StreamProducer[R]{
  override def toString: String = s"MapperProducer"
}

case class ForeachProducer[T](var fn : T => Unit) extends StreamProducer[T]
abstract class GroupByProducer[T] extends StreamProducer[T]
case class GroupByFieldProducer[T](fields : Seq[Int]) extends GroupByProducer[T]
case class GroupByStrategyProducer[T](partitionStrategy: PartitionStrategy) extends GroupByProducer[T]
case class GroupByKeyProducer[T](keySelectorFunc:T => Any) extends GroupByProducer[T]{
  override def toString: String = s"GroupByKey"
}

object GroupByProducer {
  def apply[T](fields: Seq[Int]) = new GroupByFieldProducer[T](fields)
  def apply[T](partitionStrategy : PartitionStrategy) = new GroupByStrategyProducer[T](partitionStrategy)
  def apply[T](keySelector:T => Any) = new GroupByKeyProducer[T](keySelector)
}

case class StreamUnionProducer[T1,T2,T3](others: Seq[StreamProducer[T2]]) extends StreamProducer[T3]

case class StormSourceProducer[T](source: BaseRichSpout) extends StreamProducer[T]{
  var numFields : Int = 0

  /**
    * rename outputfields to f0, f1, f2, ...
   * if one spout declare some field names, those fields names will be modified
    *
    * @param n
   */
  def withOutputFields(n : Int): StormSourceProducer[T] ={
    this.numFields = n
    this
  }
}

case class SparkStreamingSourceProducer[T]() extends StreamProducer[T]{

}

case class IterableStreamProducer[T](iterable: Iterable[T],recycle:Boolean = false) extends StreamProducer[T]{
  override def toString: String = s"IterableStreamProducer(${iterable.getClass.getSimpleName}))"
}
case class IteratorStreamProducer[T](iterator: Iterator[T]) extends StreamProducer[T]{
  override def toString: String = s"IteratorStreamProducer(${iterator.getClass.getSimpleName})"
}

case class AlertStreamProducer(var upStreamNames: util.List[String], alertExecutorId : String, var consume: Boolean=true, strategy: PartitionStrategy=null) extends StreamProducer[AlertAPIEntity] {
  def consume(consume: Boolean): AlertStreamProducer = {
    this.consume = consume
    this
  }
}

case class AggregateProducer[T](var upStreamNames: util.List[String], analyzerId : String, cepQl: String = null, strategy:PartitionStrategy = null) extends StreamProducer[T]

case class PersistProducer[T](executorId :String, storageType: StorageType.StorageType) extends StreamProducer[T]

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