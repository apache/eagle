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

import org.jgrapht.experimental.dag.DirectedAcyclicGraph

import scala.collection.JavaConverters._
import scala.collection.{JavaConversions, mutable}

/**
 * wrapper of DAG, used for storm topology compiler
 */
class StormStreamDAG(graph: DirectedAcyclicGraph[StreamProducer, StreamConnector]) extends AbstractStreamProducerGraph {
  var nodeMap: mutable.Map[String, StreamProducer] = null

  override def addEdge(from: StreamProducer, to: StreamProducer, streamConnector: StreamConnector): Unit = {
    graph.addEdge(from, to, streamConnector)
  }

  override def addVertex(producer: StreamProducer): Unit = {
    graph.addVertex(producer)
  }

  override def iterator(): Iterator[StreamProducer] = {
    JavaConversions.asScalaIterator(graph.iterator())
  }

  override def isSource(v: StreamProducer): Boolean = {
    graph.inDegreeOf(v) match {
      case 0 => true
      case _ => false
    }
  }

  override def outgoingEdgesOf(v: StreamProducer): scala.collection.mutable.Set[StreamConnector] = {
    JavaConversions.asScalaSet(graph.outgoingEdgesOf(v))
  }

  override def getNodeByName(name: String): Option[StreamProducer] = {
    nodeMap.get(name)
  }

  def setNodeMap(nodeMap: mutable.Map[String, StreamProducer]): Unit = {
    this.nodeMap = nodeMap
  }

  override def incomingVertexOf(v: StreamProducer): scala.collection.mutable.Set[StreamProducer] = {
    val set = mutable.Set[StreamProducer]()
    graph.incomingEdgesOf(v).asScala.foreach(e => set += graph.getEdgeSource(e))
    set
  }
}
