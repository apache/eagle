package org.apache.eagle.stream.dsl.builder

import org.apache.eagle.datastream.core.StreamProducer
import org.apache.eagle.stream.dsl.definition.DataStream

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
trait StreamImplicits extends StreamContextBuilder{
  implicit class DataStreamNameImplicits(name:String) {
    def := ( builder: => DataStream) :Unit = {
      val stream = builder
      stream.setName(name)
      setStream(stream)
    }

    def :=> ( builder: => DataStream) :Unit = {
      getStream(name).sink(builder)
    }

    def :=> ( producer: StreamProducer[Any]) :Unit = {
      getStream(name).sink(producer)
    }
  }
}
