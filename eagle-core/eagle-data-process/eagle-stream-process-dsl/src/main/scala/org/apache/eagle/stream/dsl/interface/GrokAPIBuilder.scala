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
package org.apache.eagle.stream.dsl.interface

import org.apache.eagle.datastream.{Collector, FlatMapper}
import org.apache.eagle.stream.dsl.definition.StreamDefinition
import org.apache.eagle.stream.dsl.utils.UtilImplicits._

import scala.collection.mutable
import scala.util.matching.Regex

case class GrokContext() extends FlatMapper[Any]{
  private var processors = mutable.ArrayBuffer[GrokProcessor]()
  def append(grok: GrokProcessor):Unit = {
    processors += grok
  }
  override def flatMap(input: Seq[AnyRef], collector: Collector[Any]): Unit = {
    processors.foreach(f=> f.process(input,collector))
  }
}

private[dsl] trait GrokProcessor{
  def process(input: Seq[AnyRef], collector: Collector[Any]):Unit
}

case class FieldPatternGrok(fieldPattern:Seq[(String,Regex)])(implicit stream:StreamDefinition) extends GrokProcessor {
  /**
   * TODO: Make sure fields in fixed order, otherwise it may cause logic bug
   *
   * @param input
   * @param collector
   */
  override def process(input: Seq[AnyRef], collector: Collector[Any]): Unit = {
    fieldPattern.foreach(pair =>{
      val field = pair._1
      val regex = pair._2
      val index = stream.getSchema.indexOfAttributeOrException(field)
      val inputMap = input.asMap(stream)
      val namedGroups = regex.namedGroups

      // TODO: Update stream schema according to namedGroups

      regex.findAllMatchIn(input(index).asInstanceOf[String]).foreach(m=>{
        val result = inputMap.clone()
        m.namedGroupsValue(regex).foreach(m =>{
          result(m._1)=m._2
        })
        collector.collect(result.values)
      })
    })
  }
}

case class AddFieldGrok(fieldPattern:Seq[(String,Any)])(implicit stream:StreamDefinition) extends GrokProcessor {
  // TODO: Implement AddFieldGrok
  override def process(input: Seq[AnyRef], collector: Collector[Any]): Unit = ???
}

trait GrokAPIBuilder extends FilterAPIBuilder{
  private var grokContext:GrokContext = null

  onInit {
    grokContext = null
  }

  def pattern(fieldPattern:(String,Regex)*):GrokContext = {
    ensure()
    grokContext.append(FieldPatternGrok(fieldPattern))
    grokContext
  }

  def add_field(fieldValue:(String,Any)*):GrokContext = {
    grokContext.append(AddFieldGrok(fieldValue))
    grokContext
  }

  def grok(func: => GrokContext):GrokAPIBuilder = {
    func
    this
  }

  override def by(grok:GrokAPIBuilder):StreamSettingAPIBuilder = {
    val producer = primaryStream.getProducer.flatMap(grok.grokContext)
    primaryStream.setProducer(producer)
    clean()
    StreamSettingAPIBuilder(primaryStream)
  }

  private def ensure():Unit = {
    if(grokContext == null) grokContext = GrokContext()
  }

  private def clean():Unit = {
    grokContext = null
  }
}