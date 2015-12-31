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
package org.apache.eagle.stream.dsl.definition

import org.apache.eagle.datastream.Collector
import org.apache.eagle.datastream.core.StreamProducer
import org.apache.eagle.stream.dsl.builder.{Processor, ProcessorContext, StreamContextBuilder}

import scala.collection.JavaConverters._

trait StreamDefinition {
  private var name:String = null
  private var schema:StreamSchema = null
  private var streamProducer:StreamProducer[Any] = null
  private var startStreamProducer:StreamProducer[Any] = null

  def setSchema(schema: StreamSchema): Unit = this.schema = schema
  def getSchema: Option[StreamSchema] = if(this.schema == null) None else Some(schema)
  def getSchemaOrException: StreamSchema = if(this.schema == null) throw new StreamUndefinedException(s"Schema of stream $this is not defined") else schema

  def setProducer(producer:StreamProducer[Any]) = this.streamProducer = producer
  def getProducer = this.streamProducer

  def getStartProducer = startStreamProducer
  def setStartProducer(producer:StreamProducer[Any]) = startStreamProducer = producer

  def setName(name:String):Unit = {
    this.name = name
  }
  def getName:String = this.name
}

class DataStream extends StreamDefinition with StreamContextBuilder with Serializable{
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

  def grok(builder: ProcessorContext):DataStream =  process(builder)

  def process(context: ProcessorContext):DataStream =  {
    newStream {stream =>
      context.setStream(stream)
      stream.setProducer(stream.getProducer.flatMap(context))
      context.close()
    }
  }

  def process(processor: Processor):DataStream =  {
    newStream {stream =>
      stream.setProducer(stream.getProducer.flatMap(processor))
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
  def |(processor: Processor) = process(processor)
  def |(processor: ProcessorContext) = process(processor)
  def | = this
  def !(executor:String) = alert(executor)

  protected def newStream(func: (DataStream)=> Unit = null):DataStream = {
    val newStream = DataStream(this)
    if(func != null) func(newStream)
    newStream
  }

  override def toString: String = s"${getClass.getSimpleName}[name = $getName, schema = $getSchema]"
}


object DataStream {
  def apply(dataStream: DataStream): DataStream = {
    val newStream = new DataStream()
    newStream.setName(dataStream.getName)
    newStream.setProducer(dataStream.getProducer)
    newStream.setSchema(dataStream.getSchema.get)
    newStream.setStartProducer(dataStream.getStartProducer)
    newStream
  }
}
