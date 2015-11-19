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

package org.apache.eagle.datastream

import java.util

import com.typesafe.config.Config

case class StormExecutorForAlertWrapper(delegate: StormStreamExecutor[Tuple2[String, util.SortedMap[AnyRef, AnyRef]]], streamName: String)
  extends StormStreamExecutor3[String, String, util.SortedMap[Object, Object]]{
  override def prepareConfig(config: Config): Unit = {
    delegate.prepareConfig(config)
  }

  override def init: Unit = {
    delegate.init
  }

  override def flatMap(input: Seq[AnyRef], collector: Collector[Tuple3[String, String, util.SortedMap[Object, Object]]]): Unit = {
    delegate.flatMap(input, new Collector[Tuple2[String, util.SortedMap[AnyRef, AnyRef]]] {
      override def collect(r: Tuple2[String, util.SortedMap[AnyRef, AnyRef]]): Unit = {
        collector.collect(Tuple3(r.f0, streamName, r.f1))
      }
    })
  }
}
