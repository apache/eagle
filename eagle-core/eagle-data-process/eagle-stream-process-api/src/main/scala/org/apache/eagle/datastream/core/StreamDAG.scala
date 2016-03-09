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

import org.jgrapht.experimental.dag.DirectedAcyclicGraph

import scala.collection.JavaConverters._
import scala.collection.{JavaConversions, mutable}

/**
 * wrapper of DAG, used for storm topology compiler
 */
class StreamDAG(val graph: DirectedAcyclicGraph[StreamProducer[Any], StreamConnector[Any,Any]]) extends StreamProducerGraph {
  var nodeMap: mutable.Map[String, StreamProducer[Any]] = null

  override def addEdge(from: StreamProducer[Any], to: StreamProducer[Any], streamConnector: StreamConnector[Any,Any]): Unit = {
    graph.addEdge(from, to, streamConnector)
  }

  override def addVertex(producer: StreamProducer[Any]): Unit = {
    graph.addVertex(producer)
  }

  override def iterator(): Iterator[StreamProducer[Any]] = {
    JavaConversions.asScalaIterator(graph.iterator())
  }

  override def isSource(v: StreamProducer[Any]): Boolean = {
    graph.inDegreeOf(v) match {
      case 0 => true
      case _ => false
    }
  }

  override def outgoingEdgesOf(v: StreamProducer[Any]): scala.collection.mutable.Set[StreamConnector[Any,Any]] = {
    JavaConversions.asScalaSet(graph.outgoingEdgesOf(v))
  }

  override def getNodeByName(name: String): Option[StreamProducer[Any]] = {
    nodeMap.get(name)
  }

  def setNodeMap(nodeMap: mutable.Map[String, StreamProducer[Any]]): Unit = {
    this.nodeMap = nodeMap
  }

  override def incomingVertexOf(v: StreamProducer[Any]): scala.collection.mutable.Set[StreamProducer[Any]] = {
    val set = mutable.Set[StreamProducer[Any]]()
    graph.incomingEdgesOf(v).asScala.foreach(e => set += graph.getEdgeSource(e))
    set
  }
}
