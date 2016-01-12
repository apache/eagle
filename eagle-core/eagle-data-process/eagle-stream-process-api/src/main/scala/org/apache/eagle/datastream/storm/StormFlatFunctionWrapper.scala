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
package org.apache.eagle.datastream.storm

import java.util

import backtype.storm.tuple.Tuple
import org.apache.eagle.datastream.Collector
import org.apache.eagle.datastream.core.StreamInfo

case class StormFlatFunctionWrapper(flatMapper:(Any,Collector[Any])=>Unit)(implicit info:StreamInfo) extends AbstractStreamBolt[Any]{
  /**
   * Handle keyed stream value
   */
  override def onKeyValue(key: Any, value: Any)(implicit input: Tuple): Unit = {
    flatMapper(value,new Collector[Any] {
      override def collect(r: Any): Unit = emit(r)(input)
    })
  }

  /**
   * Handle general stream values list
   *
   * @param values
   */
  override def onValues(values: util.List[AnyRef])(implicit input: Tuple): Unit = {
    flatMapper(values,new Collector[Any] {
      override def collect(r: Any): Unit = emit(r)(input)
    })
  }
}