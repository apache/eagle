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
package org.apache.eagle.stream.dsl.builder

import org.apache.eagle.datastream.{FlatMapper, Collector}
import org.apache.eagle.stream.dsl.definition.{StreamUndefinedException, DataStream}
import org.apache.eagle.stream.dsl.utils.UtilImplicits._

import scala.collection.mutable

trait StreamProcessorBuilder
  extends StreamProcessorContextBuilder
  with StreamGrokBuilder

trait Processor extends FlatMapper[Any]{
  override def flatMap(input: Seq[AnyRef], collector: Collector[Any]): Unit = process(input,collector)
  def process(input: Seq[AnyRef], collector: Collector[Any]):Unit
}

case class ProcessorContext() extends FlatMapper[Any] with Serializable{
  private var _processors = mutable.ArrayBuffer[Processor]()

  @transient private var _cleaners = mutable.ArrayBuffer[()=>Unit]()
  private var _stream:DataStream = null

  def setStream(stream:DataStream) = _stream = stream
  def getStream = {
    if(_stream == null) throw new NullPointerException("stream is not provided")
    _stream
  }

  def append(processor: Processor):Unit = {
    _processors += processor
    this
  }

  override def flatMap(input: Seq[AnyRef], collector: Collector[Any]): Unit = {
    _processors.foreach(_.flatMap(input,collector))
  }

  def close():Unit = _cleaners.foreach(_())
  def onClose(callback: ()=>Unit):Unit = {
    _cleaners += callback
  }
}

trait StreamProcessorContextBuilder{
  private var currentProcessorContext:ProcessorContext = null
  protected def processorContext:ProcessorContext = {
    if(currentProcessorContext == null) {
      currentProcessorContext = ProcessorContext()
      currentProcessorContext.onClose({()=>
          currentProcessorContext = null
      })
    }
    currentProcessorContext
  }
}

// Grok Processor

trait StreamGrokBuilder extends StreamProcessorContextBuilder{
  def pattern(patternMatcher:(String,String)*):ProcessorContext = {
    val ctx = processorContext
    ctx.append(PatternGrokProcessor(patternMatcher,ctx))
    ctx
  }
  
  def empty:ProcessorContext = {
    processorContext
  }
}

case class PatternGrokProcessor(patternMatcher:Seq[(String,String)],context:ProcessorContext) extends Processor{


  /**
   * TODO: Make sure fields in fixed order, otherwise it may cause logic bug
   *
   * @param input
   * @param collector
   */
  override def process(input: Seq[AnyRef], collector: Collector[Any]): Unit = {
    val stream = context.getStream

    patternMatcher.foreach(pair =>{
      val field = pair._1
      val regex = pair._2.r
      val index = stream.getSchema match {
        case Some(s) => s.indexOfAttributeOrException(field)
        case None => throw new StreamUndefinedException(s"Schema of stream $stream is not defined")
      }

      val inputMap = input.asMap(stream)

      // val namedGroups = regex.namedGroups
      // TODO: Update stream schema according to namedGroups

      regex.findAllMatchIn(input(index).asInstanceOf[String]).foreach(m=>{
        val result = inputMap.clone()
        m.namedGroupsValue(regex).foreach(m =>{result(m._1)=m._2})
        collector.collect(result.values)
      })
    })
  }
}