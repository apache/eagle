/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.eagle.stream.dsl.api

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.eagle.datastream.core._
import org.apache.eagle.datastream.{Collector, ExecutionEnvironments}
import org.apache.eagle.stream.dsl.definition.{StreamContext, StreamSchema}
import org.apache.eagle.stream.dsl.interface.SqlScript
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
import scala.reflect.runtime.{universe => ru}

trait DataStreamDefinition {
  private var name:String = null
  private var schema:StreamSchema = null
  private var streamProducer:StreamProducer[Any] = null
  private var startStreamProducer:StreamProducer[Any] = null

  def setSchema(schema: StreamSchema): Unit = this.schema = schema
  def getSchema: StreamSchema = this.schema
  def setProducer(producer:StreamProducer[Any]) = this.streamProducer = producer
  def getProducer = this.streamProducer

  def getStartProducer = startStreamProducer
  def setStartProducer(producer:StreamProducer[Any]) = startStreamProducer = producer

  def setName(name:String):Unit = {
    this.name = name
  }
  def getName:String = this.name
}

trait DataStreamContext{
  private val logger = LoggerFactory.getLogger(classOf[DataStreamContext])
  private var _context:StreamContext = null
  def context(context:StreamContext):Unit = {
    if(_context!=null && logger.isDebugEnabled()) logger.debug(s"Initializing with $context")
    _context = context
  }

  def context:StreamContext = {
    if(_context ==null) throw new IllegalStateException("Context is not initialized")
    _context
  }
}

class DataStream extends DataStreamDefinition with DataStreamContext{
  def as(attributes:(String,Symbol)*):DataStream = {
    this.setSchema(StreamSchema.build(this.getName,attributes))
    this
  }

  def parallism(num:Int):DataStream = {
    this.getProducer.parallelism(num)
    this
  }

  def from(producer:StreamProducer[Any]):DataStream = {
    this.setProducer(producer)
    this
  }

  def from(iterable:Iterable[Any]):DataStream = {
    val producer = context.getEnvironment.from(iterable)
    this.setProducer(producer)
    this
  }

  def from(product:Product):DataStream = {
    val producer = context.getEnvironment.from(product)
    this.setProducer(producer)
    this
  }

  def groupBy(fields:Int*):DataStream = {
    newStream { stream =>
      stream.setProducer(this.getProducer.groupBy(fields.map(Int.box).asJava))
    }
  }

  def filter(func: Any=>Boolean): DataStream = {
    newStream { stream =>
      stream.setProducer(stream.getProducer.filter(func))
    }
  }

  def transform(func: (Any,Collector[Any])=>Unit): DataStream = {
    newStream {stream =>
      stream.setProducer(stream.getProducer.flatMap(func))
    }
  }

  def map(func: (Any,Collector[Any])=>Unit): DataStream = transform(func)
  def alert(executor:String): DataStream = {
    newStream { stream =>
      stream.setProducer(stream.getProducer.alert(Seq(this.getName),executor))
    }
  }

  def sink(stream:DataStream):DataStream = {
    this.getProducer.connect(stream.getStartProducer)
    stream
  }

  def sink(producer:StreamProducer[Any]):DataStream = {
    this.getProducer.connect(producer)
    this
  }

  def flow(producer:StreamProducer[Any]):DataStream = {
    this.getProducer.connect(producer)
    this.setProducer(producer)
    this
  }

  def union(dataStreams:DataStream*):DataStream = {
    newStream {stream =>
      stream.setProducer(stream.getProducer.streamUnion(dataStreams.map(_.getProducer)))
    }
  }

  def ?(func: Any=>Boolean):DataStream = filter(func)
  def |(func:(Any,Collector[Any])=>Unit) = transform(func)
  def |(producer:StreamProducer[Any]) = flow(producer)
  def | = this
  def !(executor:String) = alert(executor)

  protected def newStream(func: (DataStream)=> Unit):DataStream = {
    val newStream = DataStream(this)
    func(newStream)
    newStream
  }

  override def toString: String = s"${getClass.getSimpleName}[name = $getName, schema = $getSchema]"
}

object DataStream {
  def apply(dataStream: DataStream): DataStream = {
    val newStream = new DataStream()
    newStream.setName(dataStream.getName)
    newStream.setProducer(dataStream.getProducer)
    newStream.setSchema(dataStream.getSchema)
    newStream.setStartProducer(dataStream.getStartProducer)
    newStream
  }
}

trait DataStreamExtension extends DataStreamContext{
  def stream:DataStream = {
    val stream = new DataStream()
    stream.context(context)
    stream
  }

  def stdout:StreamProducer[Any] = ForeachProducer[Any](t=>println(t))
}

trait DataStreamBuilder extends DataStreamExtension with DataStreamContext{
  private val streamManager = new DataStreamManager()
  protected def getStream(name:String) = streamManager.getStream(name)
  protected def setStream(stream:DataStream) = streamManager.setStream(stream)
  protected def clean():Unit = streamManager.clear()
  /**
   * Override App#args:Array[String] method
   *
   * @return
   */
  def init[T<:ExecutionEnvironment](args:Array[String] = Array[String]())(implicit typeTag: ru.TypeTag[T]) = {
    context(StreamContext(ExecutionEnvironments.get[T](args)))
  }

  def init[T<:ExecutionEnvironment](config:Config)(implicit typeTag: ru.TypeTag[T]) = {
    context(StreamContext(ExecutionEnvironments.get[T](config)))
  }

  def init[T<:ExecutionEnvironment](config:Config,executionEnvironment:Class[T]) = {
    context(StreamContext(ExecutionEnvironments.get[T](config,executionEnvironment)))
  }

  def submit:ExecutionEnvironment = {
    context.getEnvironment.execute()
    context.getEnvironment
  }

  implicit class DataStreamNameImplicits(name:String) {
    def := ( builder: => DataStream) :Unit = {
      val stream = builder
      stream.setName(name)
      setStream(stream)
    }

    def :=> ( builder: => DataStream) :Unit = {
      getStream(name).sink(builder)
    }

    def :=> ( producer: StreamProducer[Any]) :Unit = {
      getStream(name).sink(producer)
    }
  }
}

object DataStreamBuilder extends DataStreamBuilder {
  type storm = org.apache.eagle.datastream.storm.StormExecutionEnvironment


  // Initialize as storm environment by default
  // TODO: May need define environment from configuration or system properties
  init[storm](ConfigFactory.load())

  implicit class StringPrefix(val sc:StringContext) extends AnyVal{
    def sql(arg:Any):SqlScript = SqlScript(arg.asInstanceOf[String])
    def $(arg:Any):DataStream = {
      getStream(sc.parts(0))
    }
  }
}