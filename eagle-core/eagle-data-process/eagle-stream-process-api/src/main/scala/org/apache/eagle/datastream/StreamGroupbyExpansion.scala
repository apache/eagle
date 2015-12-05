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

import com.typesafe.config.Config
import org.jgrapht.experimental.dag.DirectedAcyclicGraph

import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer

/**
 * Replace GroupByProducer(Vertex) with StreamConnector (Edge)
 *
 * For example as to Storm, it's mainly for grouping method
 *
 * @param config context configuration
 */
class StreamGroupbyExpansion(config: Config) extends StreamDAGExpansion(config){
  override def expand(dag: DirectedAcyclicGraph[StreamProducer[Any], StreamConnector[Any,Any]]) = {
    val iter = dag.iterator()
    var toBeAddedEdges = new ListBuffer[StreamConnector[Any,Any]]
    var toBeRemovedVertex = new ListBuffer[StreamProducer[Any]]
    while(iter.hasNext) {
      val current = iter.next()
      dag.outgoingEdgesOf(current).foreach(edge => {
        val child = edge.to
        child match {
          case p : GroupByProducer[Any] => {
            dag.outgoingEdgesOf(p).foreach(c2 => {
              p match {
                case GroupByFieldProducer(fields) =>
                  toBeAddedEdges += GroupbyFieldsConnector(current, c2.to,fields)
                case GroupByStrategyProducer(strategy) =>
                  toBeAddedEdges += GroupbyStrategyConnector(current, c2.to,strategy)
                case GroupByKeyProducer(keySelector) =>
                  current.outKeyed = true
                  current.keySelector = KeySelectorImpl(keySelector)
                  c2.to.inKeyed = true
                  toBeAddedEdges += GroupbyKeyConnector(current, c2.to,keySelector)
                case _ => toBeAddedEdges += ShuffleConnector(current, c2.to)
              }
            })
            toBeRemovedVertex += p
          }
          case _ =>
        }
      })
    }

    // add back edges
    toBeAddedEdges.foreach(e => dag.addEdge(e.from, e.to, e))
    toBeRemovedVertex.foreach(v => dag.removeVertex(v))
  }
}