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
package org.apache.eagle.datastream.core

import org.apache.eagle.partition.PartitionStrategy

abstract class StreamConnector[+T1 <: Any,+T2 <: Any](val from: StreamProducer[T1], val to: StreamProducer[T2]) extends Serializable

case class ShuffleConnector[+T1 <: Any,+T2 <: Any](override val from: StreamProducer[T1], override val to: StreamProducer[T2])
  extends StreamConnector[T1,T2](from,to){
  override def toString: String = "shuffleGroup"
}

case class GroupbyFieldsConnector[+T1 <: Any,+T2 <: Any](override val from: StreamProducer[T1], override val to: StreamProducer[T2],groupByFields : Seq[Int])
  extends StreamConnector[T1,T2](from,to){
  override def toString: String = s"groupByFields( $groupByFields )"
}

case class GroupbyKeyConnector[T1 <: Any,+T2 <: Any](override val from: StreamProducer[T1], override val to: StreamProducer[T2],keySelector: T1 => Any)
  extends StreamConnector[T1,T2](from,to){
  override def toString: String = s"groupByKey($keySelector)"
}

case class GroupbyStrategyConnector[+T1 <: Any,+T2 <: Any](override val from: StreamProducer[T1], override val to: StreamProducer[T2],customGroupBy:PartitionStrategy)
  extends StreamConnector[T1,T2](from,to){
  override def toString: String = s"groupByStrategy( $customGroupBy )"
}

object StreamConnector{
  /**
   *
   * @param from
   * @param to
   * @tparam T1
   * @tparam T2
   * @return
   */
  def apply[T1 <: Any,T2 <: Any](from: StreamProducer[T1], to: StreamProducer[T2]):ShuffleConnector[T1,T2] = ShuffleConnector(from,to)

  /**
   *
   * @param from
   * @param to
   * @param groupByFields
   * @tparam T1
   * @tparam T2
   * @return
   */
  def apply[T1 <: Any,T2 <: Any](from: StreamProducer[T1], to: StreamProducer[T2],groupByFields : Seq[Int]):GroupbyFieldsConnector[T1,T2] = GroupbyFieldsConnector(from,to,groupByFields)

  /**
   *
   * @param from
   * @param to
   * @param customGroupBy
   * @tparam T1
   * @tparam T2
   * @return
   */
  def apply[T1 <: Any,T2 <: Any](from: StreamProducer[T1], to: StreamProducer[T2],customGroupBy: PartitionStrategy):GroupbyStrategyConnector[T1,T2] = GroupbyStrategyConnector(from,to,customGroupBy)
}