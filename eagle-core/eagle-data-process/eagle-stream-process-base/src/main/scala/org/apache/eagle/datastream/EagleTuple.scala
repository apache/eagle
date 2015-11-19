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

trait EagleTuple extends Serializable{
  def getList : List[AnyRef]
}

case class Tuple1[T0](f0 : T0) extends EagleTuple{
  override def getList : List[AnyRef] = {
    return List(f0.asInstanceOf[AnyRef])
  }
}

case class Tuple2[T0, T1](f0 : T0, f1: T1) extends EagleTuple{
  override def getList : List[AnyRef] = {
    return List(f0.asInstanceOf[AnyRef], f1.asInstanceOf[AnyRef])
  }
}

case class Tuple3[T0, T1, T2](f0 : T0, f1: T1, f2: T2) extends EagleTuple{
  override def getList : List[AnyRef] = {
    return List(f0.asInstanceOf[AnyRef], f1.asInstanceOf[AnyRef], f2.asInstanceOf[AnyRef])
  }
}

case class Tuple4[T0, T1, T2, T3](f0 : T0, f1: T1, f2: T2, f3 : T3) extends EagleTuple{
  override def getList : List[AnyRef] = {
    return List(f0.asInstanceOf[AnyRef], f1.asInstanceOf[AnyRef], f2.asInstanceOf[AnyRef], f3.asInstanceOf[AnyRef])
  }
}

