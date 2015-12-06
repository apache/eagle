/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.eagle.datastream

import java.util

import backtype.storm.tuple.Tuple
import org.apache.eagle.datastream.storm.AbstractStreamBolt

/**
 * @param num if num is zero, then means that it's using type-safe way, because to map operation, it must require at least one output field
 * @param fn
 * @param streamInfo
 */
case class MapBoltWrapper(num: Int, fn: Any => Any)(implicit streamInfo: StreamInfo) extends AbstractStreamBolt[Any](fieldsNum = num){
  /**
   * Handle keyed stream value
   */
  override def handleKeyValue(key: Any, value: Any)(implicit input:Tuple): Unit = {
    emit(fn(value))
  }

  /**
   * Handle general stream values list
   *
   * @param values
   */
  override def handleValues(values: util.List[AnyRef])(implicit input:Tuple): Unit = {
    val size = values.size()
    if(size == 0) return
    if(num == 0) {
      emit(fn(values.get(0)))
    } else {
      var tuple: AnyRef = null
      size match {
        case 1 => tuple = scala.Tuple1[AnyRef](values.get(0))
        case 2 => tuple = scala.Tuple2(values.get(0), values.get(1))
        case 3 => tuple = scala.Tuple3(values.get(0), values.get(1), values.get(2))
        case 4 => tuple = scala.Tuple4(values.get(0), values.get(1), values.get(2), values.get(3))
        case _ => throw new IllegalArgumentException(s"Exceed max supported tuple size $size > 4")
      }
      val output = fn(tuple)
      output match {
        case scala.Tuple1(a) => emit(util.Arrays.asList(a.asInstanceOf[AnyRef]))
        case scala.Tuple2(a, b) => emit(util.Arrays.asList(a.asInstanceOf[AnyRef], b.asInstanceOf[AnyRef]))
        case scala.Tuple3(a, b, c) => emit(util.Arrays.asList(a.asInstanceOf[AnyRef], b.asInstanceOf[AnyRef], c.asInstanceOf[AnyRef]))
        case scala.Tuple4(a, b, c, d) => emit(util.Arrays.asList(a.asInstanceOf[AnyRef], b.asInstanceOf[AnyRef], c.asInstanceOf[AnyRef], d.asInstanceOf[AnyRef]))
        case a => emit(util.Arrays.asList(a.asInstanceOf[AnyRef]))
      }
    }
  }
}