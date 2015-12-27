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
  private var grokFuncs = mutable.ArrayBuffer[GrokFunction]()
  def appendGrok(grok: GrokFunction):Unit = {
    grokFuncs += grok
  }

  override def flatMap(input: Seq[AnyRef], collector: Collector[Any]): Unit = {
    grokFuncs.foreach(f=> f.flatMap(input,collector))
  }
}

trait GrokFunction extends FlatMapper[Any]

case class FieldPatternGrok(fieldPattern:Seq[(String,Regex)])(implicit stream:StreamDefinition) extends GrokFunction {
  override def flatMap(input: Seq[AnyRef], collector: Collector[Any]): Unit = {
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

//case class AddFieldGrok(fieldPattern:Seq[(String,Any)])(implicit stream:StreamDefinition) extends GrokFunc {
//  override def flatMap(input: Seq[AnyRef], collector: Collector[Any]): Unit = {
//    //
//  }
//}

trait GrokAPIBuilder extends FilterAPIBuilder{
  private var grokContext:GrokContext = null

  def pattern(fieldPattern:(String,Regex)*):GrokContext = {
    ensureGrok()
    grokContext.appendGrok(FieldPatternGrok(fieldPattern))
    grokContext
  }

//  def add_field(fieldValue:(String,Any)*):GrokDefinition = {
//    _grokDefinition.addGrok(AddFieldGrok(fieldValue))
//    _grokDefinition
//  }

  def grok(func: => GrokContext):GrokAPIBuilder = {
    func
    this
  }

  override def by(grok:GrokAPIBuilder):StreamSettingAPIBuilder = {
    val producer = primaryStream.getProducer.flatMap(grok.grokContext)
    primaryStream.setProducer(producer)
    cleanGrok()
    StreamSettingAPIBuilder(primaryStream)
  }

  private def ensureGrok():Unit = {
    if(grokContext == null) grokContext = GrokContext()
  }

  private def cleanGrok():Unit = {
    grokContext = null
  }
}