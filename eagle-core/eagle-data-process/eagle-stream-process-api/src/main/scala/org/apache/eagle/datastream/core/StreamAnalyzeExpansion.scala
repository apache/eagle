
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
 * TODO : should re-use flow with stream alert expansion, make code cleaner
 */
class StreamAnalyzeExpansion(config: Config) extends StreamAlertExpansion(config) {

  override def onIteration(toBeAddedEdges: ListBuffer[StreamConnector[Any, Any]], toBeRemovedVertex: ListBuffer[StreamProducer[Any]],
    dag: DirectedAcyclicGraph[StreamProducer[Any], StreamConnector[Any, Any]], current: StreamProducer[Any],
    child: StreamProducer[Any]): Unit = {
    child match {
      case AnalyzeProducer(upStreamNames, analyzerId, cepQl, strategy) => {
        /**
         * Rewrite the tree to add output field wrapper since policy executors accept only fixed tuple format 
         */
        val newStreamProducers = rewriteWithStreamOutputWrapper(current, dag, toBeAddedEdges, toBeRemovedVertex, upStreamNames)
        
        val analyzeExecutors = AnalyzeExecutorFactory.Instance.createExecutors(config, upStreamNames, analyzerId);
        analyzeExecutors.foreach(exec => {
          val t = FlatMapProducer(exec).nameAs(exec.getExecutorId() + "_" + exec.getPartitionSeq()).initWith(dag,config, hook = false)

          // connect with previous
          if (strategy == null) {
            newStreamProducers.foreach(s => toBeAddedEdges += StreamConnector(s, t))
          } else {
            newStreamProducers.foreach(s => toBeAddedEdges += StreamConnector(s, t, strategy))
          }

          // connect with next
          val outgoingEdges = dag.outgoingEdgesOf(child)
          outgoingEdges.foreach(e => toBeAddedEdges += StreamConnector(t, e.to, e))
        })
        
        // remote current child
        toBeRemovedVertex += child
      }
      case _ => 
    }
  }
  
}

object StreamAnalyzeExpansion{
  def apply()(implicit config:Config, dag: DirectedAcyclicGraph[StreamProducer[Any], StreamConnector[Any,Any]]): StreamAnalyzeExpansion ={
    val e = new StreamAnalyzeExpansion(config)
    e.expand(dag)
    e
  }
}