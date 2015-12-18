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
package org.apache.eagle.datastream.core

import scala.collection.JavaConversions.asScalaSet
import scala.collection.mutable.ListBuffer

import org.apache.eagle.dataproc.impl.analyze.AnalyzeExecutorFactory
import org.jgrapht.experimental.dag.DirectedAcyclicGraph

import com.typesafe.config.Config


/**
 * The expansion job for stream analyze
 * 
 * TODO : should re-use flow with stream alert expansion
 */
class StreamAnalyzeExpansion(config: Config) extends StreamDAGExpansion(config) {

  def expand(dag: DirectedAcyclicGraph[StreamProducer[Any], StreamConnector[Any,Any]]) {
    val iter = dag.iterator()
    val toBeAddedEdges = new ListBuffer[StreamConnector[Any, Any]]
    val toBeRemovedVertex = new ListBuffer[StreamProducer[Any]]
    while (iter.hasNext) {
      onIteration(toBeAddedEdges, toBeRemovedVertex, dag, iter.next())
    }
    // add back edges
    toBeAddedEdges.foreach(e => {
      dag.addVertex(e.from)
      dag.addVertex(e.to)
      dag.addEdge(e.from, e.to, e)
    })
    toBeRemovedVertex.foreach(v => dag.removeVertex(v))
  }

  def onIteration(toBeAddedEdges: ListBuffer[StreamConnector[Any, Any]], toBeRemovedVertex: ListBuffer[StreamProducer[Any]],
    dag: DirectedAcyclicGraph[StreamProducer[Any], StreamConnector[Any, Any]], current: StreamProducer[Any]): Unit = {
    current match {
      case AnalyzeProducer(upStreamNames, analyzerId, cepQl, strategy) => {
        val analyzeExecutors = AnalyzeExecutorFactory.createExecutors();
        analyzeExecutors.foreach(exec => {
          val t = FlatMapProducer(exec).nameAs(exec.getExecutorId() + "_" + exec.getPartitionSeq()).initWith(dag,config, hook = false)

          // connect with previous
          val incomingEdges = dag.incomingEdgesOf(current)
          incomingEdges.foreach(e => toBeAddedEdges += StreamConnector(e.from, t, e))

          // connect with next
          val outgoingEdges = dag.outgoingEdgesOf(current)
          outgoingEdges.foreach(e => toBeAddedEdges += StreamConnector(t, e.to, e))
        })

        toBeRemovedVertex += current
      }
      case _ => 
    }
  }
  
}

object StreamAnalyzeExpansion{
  def apply()(implicit config:Config, dag: DirectedAcyclicGraph[StreamProducer[Any], StreamConnector[Any,Any]]): StreamAlertExpansion ={
    val e = StreamAlertExpansion(config)
    e.expand(dag)
    e
  }
}