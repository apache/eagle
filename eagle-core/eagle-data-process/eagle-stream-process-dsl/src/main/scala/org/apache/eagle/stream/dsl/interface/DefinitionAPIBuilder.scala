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

trait DefinitionAPIBuilder extends BaseAPIBuilder{
  private var _instance:StreamDefinition = null
  def define(name:String):DefinitionAPIBuilder = {
    _instance = StreamDefinition(name)
    context.getStreamManager.setStreamDefinition(name,_instance)
    this
  }

  def define(name:String,attributes:Seq[(String,AnyRef)]):DefinitionAPIBuilder = {
    _instance = StreamDefinition(name,StreamSchema.build(name,attributes))
    context.getStreamManager.setStreamDefinition(name,_instance)
    this
  }

  def show(name:String): Unit = {
    println(context.getStreamManager.getStreamDefinition(name))
  }

  def as(attributes:(String,AnyRef)*):DefinitionAPIBuilder = {
    shouldNotBeNull(_instance)
    _instance.setSchema(StreamSchema.build(_instance.name,attributes))
    this
  }

  def from(source:StreamProducer[AnyRef]):ProducerSettingAPIBuilder = {
    shouldNotBeNull(_instance)
    _instance.setProducer(source)
    ProducerSettingAPIBuilder(source)
  }

  def from(iterable:Iterable[AnyRef],recycle:Boolean=true):ProducerSettingAPIBuilder = {
    val producer = context.getEnvironment.from(iterable,recycle)
    _instance.setProducer(producer)
    ProducerSettingAPIBuilder(producer)
  }

  def parallism(num:Int):DefinitionAPIBuilder = {
    shouldNotBeNull(_instance)
    _instance.getProducer.parallelism(num)
    this
  }
}