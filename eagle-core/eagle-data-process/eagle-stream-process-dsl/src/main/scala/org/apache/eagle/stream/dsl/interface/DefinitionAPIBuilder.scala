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

import org.apache.eagle.datastream.core.StreamProducer
import org.apache.eagle.stream.dsl.definition.{StreamDefinition, StreamSchema}

trait DefinitionAPIBuilder extends AbstractAPIBuilder{
  def define(name:String):DefinitionAPIBuilder = {
    primaryStream = new StreamDefinition(name)
    context.getStreamManager.setStreamDefinition(name,primaryStream)
    this
  }

  def define(name:String,attributes:Seq[(String,AnyRef)]):DefinitionAPIBuilder = {
    primaryStream = new StreamDefinition(name,StreamSchema.build(name,attributes))
    context.getStreamManager.setStreamDefinition(name,primaryStream)
    this
  }

  def as(attributes:(String,AnyRef)*):DefinitionAPIBuilder = {
    shouldNotBeNull(primaryStream)
    primaryStream.setSchema(StreamSchema.build(primaryStream.getName,attributes))
    this
  }

  def from(source:StreamProducer[AnyRef]):StreamSettingAPIBuilder = {
    shouldNotBeNull(primaryStream)
    primaryStream.setProducer(source)
    StreamSettingAPIBuilder(primaryStream)
  }

  def from(iterable:Iterable[Any]):StreamSettingAPIBuilder = {
    val producer = context.getEnvironment.from(iterable,recycle = false)
    primaryStream.setProducer(producer)
    StreamSettingAPIBuilder(primaryStream)
  }

  def from(product:Product):StreamSettingAPIBuilder = {
    val producer = context.getEnvironment.from(product)
    primaryStream.setProducer(producer)
    StreamSettingAPIBuilder(primaryStream)
  }

  def parallism(num:Int):DefinitionAPIBuilder = {
    shouldNotBeNull(primaryStream)
    primaryStream.getProducer.parallelism(num)
    this
  }

//  implicit class StreamDefinitionImplicits(name:String) {
//    def :=(source:StreamProducer[AnyRef]) :StreamSettingAPIBuilder = define(name).from(source)
//    def :=(iterable:Iterable[Any]) :StreamSettingAPIBuilder = define(name).from(iterable)
//    def :=(product:Product) :StreamSettingAPIBuilder = define(name).from(product)
//  }
}

case class StreamSettingAPIBuilder(stream:StreamDefinition){
  def parallism(num:Int):StreamSettingAPIBuilder = {
    stream.getProducer.parallelism(num)
    this
  }

  def as(attributes:(String,AnyRef)*):StreamSettingAPIBuilder = {
    stream.setSchema(StreamSchema.build(stream.getName,attributes))
    this
  }
}