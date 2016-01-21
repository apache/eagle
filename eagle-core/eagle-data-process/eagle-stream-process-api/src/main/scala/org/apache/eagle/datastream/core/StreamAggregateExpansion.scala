
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

import com.typesafe.config.Config
import org.apache.eagle.dataproc.impl.aggregate.AggregateExecutorFactory
import org.apache.eagle.datastream.FlatMapper
import org.jgrapht.experimental.dag.DirectedAcyclicGraph

import scala.collection.JavaConversions.asScalaSet
import scala.collection.mutable.ListBuffer

/**
 * The expansion job for stream analyze
 * 
 * TODO : should re-use flow with stream alert expansion, make code cleaner
 */
class StreamAggregateExpansion(config: Config) extends StreamAlertExpansion(config) {

  override def onIteration(toBeAddedEdges: ListBuffer[StreamConnector[Any, Any]], toBeRemovedVertex: ListBuffer[StreamProducer[Any]],
    dag: DirectedAcyclicGraph[StreamProducer[Any], StreamConnector[Any, Any]], current: StreamProducer[Any],
    child: StreamProducer[Any]): Unit = {
    child match {
      case AggregateProducer(upStreamNames, analyzerId, cepQl, strategy) => {
        /**
         * Rewrite the tree to add output field wrapper since policy executors accept only fixed tuple format 
         */
        val newStreamProducers = rewriteWithStreamOutputWrapper(current, dag, toBeAddedEdges, toBeRemovedVertex, upStreamNames)

        val analyzeExecutors = if (cepQl != null) {
          AggregateExecutorFactory.Instance.createExecutors(cepQl,upStreamNames)
        } else {
          AggregateExecutorFactory.Instance.createExecutors(config, upStreamNames, analyzerId)
        }

        analyzeExecutors.foreach(exec => {
          val t = FlatMapProducer(exec.asInstanceOf[FlatMapper[Any]]).initWith(dag,config, hook = false).nameAs(exec.getExecutorId + "_" + exec.getPartitionSeq).stream(child.stream)

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

object StreamAggregateExpansion{
  def apply()(implicit config:Config, dag: DirectedAcyclicGraph[StreamProducer[Any], StreamConnector[Any,Any]]): StreamAggregateExpansion ={
    val e = new StreamAggregateExpansion(config)
    e.expand(dag)
    e
  }
}