/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one or more
 *  * contributor license agreements.  See the NOTICE file distributed with
 *  * this work for additional information regarding copyright ownership.
 *  * The ASF licenses this file to You under the Apache License, Version 2.0
 *  * (the "License"); you may not use this file except in compliance with
 *  * the License.  You may obtain a copy of the License at
 *  *
 *  *    http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */
package org.apache.eagle.datastream.utils

import java.util

import org.apache.eagle.datastream.core.StreamProducer

import scala.collection.JavaConverters._

object UnionUtils {
  def join[T1,T2](producers : StreamProducer[T1]*) : StreamProducer[T2] = {
    producers.head.streamUnion(producers.drop(1))
  }

  def join[T1,T2](producers : java.util.List[StreamProducer[T1]]) : StreamProducer[T2] = {
    val newList = new util.ArrayList(producers)
    val head = newList.get(0)
    newList.remove(0)
    head.streamUnion(newList.asScala);
  }

  def join[T1,T2](producers : List[StreamProducer[T1]]) : StreamProducer[T2] = {
    val head = producers.head
    head.streamUnion(producers.tail);
  }
}
