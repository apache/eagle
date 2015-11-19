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
import org.slf4j.LoggerFactory
import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer

/**
 * Replace GroupByProducer(Vertex) with StreamConnector (Edge)
 * @param config context configuration
 */
class StreamGroupbyExpansion(config: Config) extends StreamDAGExpansion(config){
  val LOG = LoggerFactory.getLogger(classOf[StreamGroupbyExpansion])

  override def expand(dag: DirectedAcyclicGraph[StreamProducer, StreamConnector]) = {
    val iter = dag.iterator()
    var toBeAddedEdges = new ListBuffer[StreamConnector]
    var toBeRemovedVertex = new ListBuffer[StreamProducer]
    while(iter.hasNext) {
      val current = iter.next()
      dag.outgoingEdgesOf(current).foreach(edge => {
        val child = edge.to
        child match {
          case p : GroupByProducer => {
            dag.outgoingEdgesOf(p).foreach(c2 => {
              toBeAddedEdges += StreamConnector(current, c2.to).groupBy(p.fields)
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