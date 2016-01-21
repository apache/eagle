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
package org.apache.eagle.datastream.core

import org.jgrapht.experimental.dag.DirectedAcyclicGraph

import scala.reflect.runtime.{universe => ru}

/**
 * @since  12/7/15
 */
trait StreamSourceBuilder {
  def config:Configuration

  /**
   * Business logic DAG
   * @return
   */
  def dag:DirectedAcyclicGraph[StreamProducer[Any], StreamConnector[Any,Any]]

  /**
   *
   * @param iterable top level Iterable interface
   * @param recycle
   * @tparam T
   * @return
   */
  def from[T:ru.TypeTag](iterable: Iterable[T],recycle:Boolean = false):IterableStreamProducer[T]={
    val p = IterableStreamProducer[T](iterable,recycle)
    p.initWith(dag,config.get)
    p
  }

  def from[T:ru.TypeTag](iterator: Iterator[T],recycle:Boolean):IteratorStreamProducer[T]={
    val p = IteratorStreamProducer[T](iterator)
    p.initWith(dag,config.get)
    p
  }

  def from(product: Product):IteratorStreamProducer[Any]={
    val p = IteratorStreamProducer[Any](product.productIterator)
    p.initWith(dag,config.get)
    p
  }

  def register[T](producer:StreamProducer[T]):Unit = {
    producer.initWith(dag,config.get)
  }
}