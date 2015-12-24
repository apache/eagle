package org.apache.eagle.stream.dsl.interface

import org.apache.eagle.datastream.core.StreamProducer

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

import scala.collection.JavaConversions._

private[dsl] case class ProducerSettingAPIBuilder(producer:StreamProducer[Any]) {
  def parallism(num:Int):ProducerSettingAPIBuilder = {
    producer.parallelism(num)
    this
  }
}

trait ConnectAPIBuilder extends BaseAPIBuilder {
  private var _current:StreamProducer[Any]  = null

  def to(next: StreamProducer[Any]): ProducerSettingAPIBuilder = {
    _current.connect(next)
    _current = next
    ProducerSettingAPIBuilder(next)
  }

  implicit class StreamConnectImplicits(name:String) {
    private val outer:ConnectAPIBuilder = ConnectAPIBuilder.this
    outer._current = outer.context.getStreamManager.getStreamDefinition(name).getProducer

    def to(producer: StreamProducer[AnyRef]): ProducerSettingAPIBuilder = {
      outer.to(producer)
    }

    def groupBy(fields:Int*):ConnectAPIBuilder = {
      outer._current = outer._current.groupBy(fields.map(_.asInstanceOf[Integer]))
      outer
    }

    def groupBy(func:Any => Any):ConnectAPIBuilder = {
      outer._current = outer._current.groupByKey(func)
      outer
    }
  }
}