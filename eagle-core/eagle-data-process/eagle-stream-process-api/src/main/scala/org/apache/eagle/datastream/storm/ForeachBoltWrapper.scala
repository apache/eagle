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
import org.apache.eagle.datastream.core.StreamInfo

/**
 * @since  12/6/15
 */
case class ForeachBoltWrapper(fn:Any=>Unit)(implicit info:StreamInfo) extends AbstractStreamBolt[Any]  {
  /**
   * Handle keyed stream value
   * @param value
   */
  override def onKeyValue(key:Any,value: Any)(implicit input:Tuple): Unit = {
    fn(value)
  }

  /**
   * Handle non-keyed stream values list
   *
   * @param values
   */
  override def onValues(values: util.List[AnyRef])(implicit input:Tuple): Unit = {
    fn(values)
  }
}