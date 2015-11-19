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

import com.typesafe.config.Config

trait StormStreamExecutor[R <: EagleTuple] extends FlatMapper[Seq[AnyRef], R] {
  def prepareConfig(config : Config)
  def init
  def fields : Array[String]
}

trait JavaStormStreamExecutor[R <: EagleTuple] extends FlatMapper[java.util.List[AnyRef], R] {
  def prepareConfig(config : Config)
  def init
  def fields : Array[String]
  override def toString() = this.getClass.getSimpleName
}

abstract class StormStreamExecutor1[T0] extends StormStreamExecutor[Tuple1[T0]] {
  override def fields = Array("f0")
}

abstract class JavaStormStreamExecutor1[T0] extends JavaStormStreamExecutor[Tuple1[T0]] {
  override def fields = Array("f0")
}

abstract class  StormStreamExecutor2[T0, T1] extends StormStreamExecutor[Tuple2[T0, T1]] {
  override def fields = Array("f0", "f1")
}

abstract class  JavaStormStreamExecutor2[T0, T1] extends JavaStormStreamExecutor[Tuple2[T0, T1]] {
  override def fields = Array("f0", "f1")
}

abstract class  StormStreamExecutor3[T0, T1, T2] extends StormStreamExecutor[Tuple3[T0, T1, T2]] {
  override def fields = Array("f0", "f1", "f2")
}

abstract class  JavaStormStreamExecutor3[T0, T1, T2] extends JavaStormStreamExecutor[Tuple3[T0, T1, T2]] {
  override def fields = Array("f0", "f1", "f2")
}

abstract class  StormStreamExecutor4[T0, T1, T2, T3] extends StormStreamExecutor[Tuple4[T0, T1, T2, T3]] {
  override def fields = Array("f0", "f1", "f2", "f3")
}

abstract class  JavaStormStreamExecutor4[T0, T1, T2, T3] extends JavaStormStreamExecutor[Tuple4[T0, T1, T2, T3]] {
  override def fields = Array("f0", "f1", "f2", "f3")
}