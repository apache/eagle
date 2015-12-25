package org.apache.eagle.stream.dsl.interface

import org.apache.eagle.datastream.core.StreamProducer
import org.apache.eagle.stream.dsl.definition.StreamDefinition
import org.slf4j.LoggerFactory

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
  private val LOG = LoggerFactory.getLogger(classOf[ConnectAPIBuilder])
  protected var primaryStream:StreamDefinition  = null

  def to(next: StreamProducer[Any]): StreamSettingAPIBuilder = {
    LOG.info(s"${primaryStream.getProducer} -> $next")
    primaryStream.getProducer.connect(next)
    primaryStream.setProducer(next)
    StreamSettingAPIBuilder(primaryStream)
  }

  implicit class StreamConnectImplicits(name:String) {
    private val self:ConnectAPIBuilder = ConnectAPIBuilder.this

    self.primaryStream = self.context.getStreamManager.getStreamDefinition(name)

    def to(producer: StreamProducer[Any]): StreamSettingAPIBuilder = {
      self.to(producer)
    }

    def ~>(producer: StreamProducer[Any]): StreamSettingAPIBuilder = to(producer)
    def groupBy(fields:Any*):ConnectAPIBuilder = {
      self.primaryStream.setProducer(self.primaryStream.getProducer.groupByFieldIndex(fields.map {
        case name: String => {
          val index = self.primaryStream.getSchema.indexOfAttribute(name)
          if (index < 0) throw new IllegalArgumentException(s"Attribute $name is not found in stream ${self.primaryStream}")
          index
        }
        case index: Int => index
        case f@_ => throw new IllegalArgumentException(s"Illegal field type $f, support: String, Int ")
      }))
      self
    }
    def groupBy(func:Any => Any):ConnectAPIBuilder = {
      self.primaryStream.setProducer(self.primaryStream.getProducer.groupByKey(func))
      self
    }
  }

  implicit class StreamSymbolConnectImplicits(name:Symbol) extends StreamConnectImplicits(name.name)
}