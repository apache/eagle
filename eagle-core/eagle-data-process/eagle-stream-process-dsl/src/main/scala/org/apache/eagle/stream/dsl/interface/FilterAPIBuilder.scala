package org.apache.eagle.stream.dsl.interface

import org.apache.eagle.datastream.Collector

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

trait FilterAPIBuilder extends AbstractAPIBuilder{
  def filter(streamName:String):FilterAPIBuilder = {
    primaryStream = context.getStreamManager.getStreamDefinition(streamName)
    this
  }

  def where(func:Any=>Boolean):FilterAPIBuilder = {
    val producer = primaryStream.getProducer.filter(func)
    primaryStream.setProducer(producer)
    this
  }

  def by(func:(Any,Collector[Any])=>Unit):StreamSettingAPIBuilder = {
    val producer = primaryStream.getProducer.flatMap(func)
    primaryStream.setProducer(producer)
    StreamSettingAPIBuilder(primaryStream)
  }

  def by(grok:GrokAPIBuilder):StreamSettingAPIBuilder
}