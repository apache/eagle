package org.apache.eagle.stream.dsl.interface

import org.apache.eagle.datastream.core.StreamProducer
import org.apache.eagle.stream.dsl.definition.StreamDefinition

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

trait ConnectAPIBuilder extends AbstractAPIBuilder {
  private implicit var current:StreamDefinition  = null

  def to(next: StreamProducer[Any]): StreamSettingAPIBuilder = {
    current.getProducer.connect(next)
    current.setProducer(next)
    StreamSettingAPIBuilder(current)
  }

  implicit class StreamConnectImplicits(name:String) {
    private val self:ConnectAPIBuilder = ConnectAPIBuilder.this
    self.current = self.context.getStreamManager.getStreamDefinition(name)
    def to(producer: StreamProducer[Any]): StreamSettingAPIBuilder = {
      self.to(producer)
    }
    def ~>(producer: StreamProducer[Any]): StreamSettingAPIBuilder = to(producer)
    def groupBy(fields:Any*):ConnectAPIBuilder = {
      self.current.setProducer(self.current.getProducer.groupByFieldIndex(fields.map {
        case name: String => {
          val index = self.current.getSchema.indexOfAttribute(name)
          if (index < 0) throw new IllegalArgumentException(s"Attribute $name is not found in stream ${self.current}")
          index
        }
        case index: Int => index
        case f@_ => throw new IllegalArgumentException(s"Illegal field type $f, support: String, Int ")
      }))
      self
    }
    def groupBy(func:Any => Any):ConnectAPIBuilder = {
      self.current.setProducer(self.current.getProducer.groupByKey(func))
      self
    }
  }
}