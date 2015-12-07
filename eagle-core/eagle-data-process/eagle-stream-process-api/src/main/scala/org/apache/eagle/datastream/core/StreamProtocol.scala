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

import com.typesafe.config.Config
import org.apache.eagle.datastream.utils.Reflections
import org.apache.eagle.datastream.{FlatMapper, JavaStreamProtocol}
import org.apache.eagle.partition.PartitionStrategy
import org.jgrapht.experimental.dag.DirectedAcyclicGraph

import scala.reflect.runtime.{universe => ru}

/**
 * StreamInfo should be fully serializable and having not runtime type information
 */
class StreamInfo  extends Serializable{
  /**
   * Processing Element Id
   */
  val id:Int = UniqueId.incrementAndGetId()

  /**
   * Processing Element Name
   */
  var name: String = null

  var streamId:String=null
  var parallelismNum: Int = 1

  /**
   * Keyed input stream
   */
  var inKeyed:Boolean = false
  /**
   * Keyed output stream
   */
  var outKeyed:Boolean = false
  /**
   * Output key selector
   */
  var keySelector:KeySelector = null

  /**
   * Entity class type of T
   */
  var typeClass:Class[_] = null

  /**
   * Entity typeTag of T (un-serializable)
   */
  @transient var typeTag:ru.TypeTag[_] = null

  /**
   * Handle unsupported issues like serializing un-serializable variable
   */
  def reinit():Unit = {
    this.typeTag = Reflections.typeTag(this.typeClass)
  }

  def getInfo:StreamInfo = this
}

/**
 * Stream interaction protocol interface
 *
 * @tparam T processed elements type
 */
trait StreamProtocol[+T <: Any] extends JavaStreamProtocol{
  /**
   * Initialize the stream metadata info
   */
  def init[E:ru.TypeTag](graph:DirectedAcyclicGraph[StreamProducer[Any], StreamConnector[Any,Any]],config:Config):Unit

  /**
   * Support Java API
   *
   * @param flatMapper
   * @tparam R
   * @return
   */
  def flatMap[R:ru.TypeTag](flatMapper : FlatMapper [R]): StreamProducer[R]

  /**
   *
   * @param fn
   * @return
   */
  def filter(fn : T => Boolean): StreamProducer[T]

  /**
   *
   * @param fn
   */
  def foreach(fn : T => Unit) : Unit

  /**
   * Type safe mapper
   * @param fn
   * @tparam R
   * @return
   */
  def map[R:ru.TypeTag](fn : T => R): StreamProducer[R]

  /**
   * Field base mapper
   * @param fn
   * @tparam R
   * @return
   */
  def map1[R:ru.TypeTag](fn : T => R) : StreamProducer[R]
  def map2[R:ru.TypeTag](fn : T => R) : StreamProducer[R]
  def map3[R:ru.TypeTag](fn : T => R) : StreamProducer[R]
  def map4[R:ru.TypeTag](fn : T => R) : StreamProducer[R]

  def groupBy(fields : Int*) : StreamProducer[T]
  def groupBy(fields : java.util.List[Integer]) : StreamProducer[T]
  def groupBy(strategy : PartitionStrategy) : StreamProducer[T]

  /**
   * @param keyer key selector function
   * @return
   */
  def groupByKey(keyer:T => Any):StreamProducer[T]
  def union[T2,T3:ru.TypeTag](otherStreams : Seq[StreamProducer[T2]]) : StreamProducer[T3]
  def alert(upStreamNames: Seq[String], alertExecutorId : String, consume: Boolean,strategy : PartitionStrategy)
  /**
   * Set processing element parallelism setting
   * @param parallelismNum parallelism value
   * @return
   */
  def parallelism(parallelismNum : Int) : StreamProducer[T]
  def parallelism : Int
  /**
   * Set component name
   *
   * @param componentName
   * @return
   */
  def nameAs(componentName : String) : StreamProducer[T]

  /**
   * Set stream name
   * @param streamId stream ID
   * @return
   */
  def stream(streamId: String): StreamProducer[T]
  def stream: String

  def ? (fn:T => Boolean):StreamProducer[T] = this.filter(fn)
  def ~>[R:ru.TypeTag](flatMapper : FlatMapper [R]) = this.flatMap[R](flatMapper)
  def ! (upStreamNames: Seq[String], alertExecutorId : String, consume: Boolean = true,strategy: PartitionStrategy = null) = alert(upStreamNames, alertExecutorId, consume,strategy)
}