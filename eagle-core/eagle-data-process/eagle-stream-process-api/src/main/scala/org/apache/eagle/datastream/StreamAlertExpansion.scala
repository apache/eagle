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

import java.util

import com.typesafe.config.Config
import org.apache.eagle.alert.dao.AlertDefinitionDAOImpl
import org.apache.eagle.executor.AlertExecutorCreationUtils
import org.jgrapht.experimental.dag.DirectedAcyclicGraph
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer

/**
 * The constraints for alert is:
 * 1. only 3 StreamProducers can be put immediately before MapProducer, FlatMapProducer, StreamUnionProducer
 * 2. For StreamUnionProducer, the only supported unioned producers are MapProducer and FlatMapProducer
 * 3. the output for MapProducer and FlatMapProducer is 2-field tuple, key and value, key is string, value has to be SortedMap
 * 4. the framework will wrapper original MapProducer and FlatMapProducer to emit 3-field tuple, {key, streamName and value}
 * 5. the framework will automatically partition traffic with first field
 *
 *
 * 2 steps
 * step 1: wrapper previous StreamProducer with one more field "streamName"
 * step 2: partition alert executor by policy partitioner class
 */

class StreamAlertExpansion(config: Config) extends StreamDAGExpansion(config) {
  val LOG = LoggerFactory.getLogger(classOf[StreamAlertExpansion])

  override def expand(dag: DirectedAcyclicGraph[StreamProducer, StreamConnector]): Unit ={
    val iter = dag.iterator()
    val toBeAddedEdges = new ListBuffer[StreamConnector]
    val toBeRemovedVertex = new ListBuffer[StreamProducer]
    while(iter.hasNext) {
      val current = iter.next()
      dag.outgoingEdgesOf(current).foreach(edge => {
        val child = edge.to
        onIteration(toBeAddedEdges, toBeRemovedVertex, dag, current, child)
      })
    }
    // add back edges
    toBeAddedEdges.foreach(e => {
      dag.addVertex(e.from)
      dag.addVertex(e.to)
      dag.addEdge(e.from, e.to, e)
    })
    toBeRemovedVertex.foreach(v => dag.removeVertex(v))
  }

  def onIteration(toBeAddedEdges: ListBuffer[StreamConnector], toBeRemovedVertex: ListBuffer[StreamProducer],
               dag: DirectedAcyclicGraph[StreamProducer, StreamConnector], current: StreamProducer, child: StreamProducer): Unit = {
    child match {
      case AlertStreamSink(id, upStreamNames, alertExecutorId, withConsumer) => {
        /**
         * step 1: wrapper previous StreamProducer with one more field "streamName"
         * for AlertStreamSink, we check previous StreamProducer and replace that
         */
        val newStreamProducers = new ListBuffer[StreamProducer]
        current match {
          case StreamUnionProducer(id, others) => {
            val incomingEdges = dag.incomingEdgesOf(current)
            incomingEdges.foreach(e => newStreamProducers += replace(toBeAddedEdges, toBeRemovedVertex, dag, e.from, upStreamNames.get(0)))
            var i: Int = 1
            others.foreach(o => {
              newStreamProducers += replace(toBeAddedEdges, toBeRemovedVertex, dag, o, upStreamNames.get(i))
              i += 1
            })
          }
          case _: FlatMapProducer[AnyRef, AnyRef] => {
            newStreamProducers += replace(toBeAddedEdges, toBeRemovedVertex, dag, current, upStreamNames.get(0))
          }
          case _: MapProducer => {
            newStreamProducers += replace(toBeAddedEdges, toBeRemovedVertex, dag, current, upStreamNames.get(0))
          }
          case s: StreamProducer if dag.inDegreeOf(s) == 0 => {
            newStreamProducers += replace(toBeAddedEdges, toBeRemovedVertex, dag, current, upStreamNames.get(0))
          }
          case p@_ => throw new IllegalStateException(s"$p can not be put before AlertStreamSink, only StreamUnionProducer,FlatMapProducer and MapProducer are supported")
        }

        /**
         * step 2: partition alert executor by policy partitioner class
         */
        val alertExecutors = AlertExecutorCreationUtils.createAlertExecutors(config, new AlertDefinitionDAOImpl(config), upStreamNames, alertExecutorId)
        var alertProducers = new scala.collection.mutable.MutableList[StreamProducer]
        alertExecutors.foreach(exec => {
          val t = FlatMapProducer(UniqueId.incrementAndGetId(), exec).withName(exec.getAlertExecutorId() + "_" + exec.getPartitionSeq())
          t.setConfig(config)
          t.setGraph(dag)
          alertProducers += t
          newStreamProducers.foreach(newsp => toBeAddedEdges += StreamConnector(newsp, t).groupBy(Seq(0)))
        })

        // remove AlertStreamSink
        toBeRemovedVertex += child

        // add alert consumer if necessary
        if (withConsumer) {
          AlertExecutorConsumerUtils.setupAlertConsumers(toBeAddedEdges, alertProducers.toList)
        }
      }
      case _ =>
    }
  }

  private def replace(toBeAddedEdges: ListBuffer[StreamConnector], toBeRemovedVertex: ListBuffer[StreamProducer],
                      dag: DirectedAcyclicGraph[StreamProducer, StreamConnector], current: StreamProducer, upStreamName: String) : StreamProducer= {
    var newsp: StreamProducer = null
    current match {
      case _: FlatMapProducer[AnyRef, AnyRef] => {
        val mapper = current.asInstanceOf[FlatMapProducer[_, _]].mapper
        mapper match {
          case a: JavaStormStreamExecutor[EagleTuple] => {
            val newmapper = new JavaStormExecutorForAlertWrapper(a.asInstanceOf[JavaStormStreamExecutor[Tuple2[String, util.SortedMap[AnyRef, AnyRef]]]], upStreamName)
            newsp = FlatMapProducer(UniqueId.incrementAndGetId(), newmapper)
            newsp.setGraph(dag)
            newsp.setConfig(config)
          }
          case b: StormStreamExecutor[EagleTuple] => {
            val newmapper = StormExecutorForAlertWrapper(b.asInstanceOf[StormStreamExecutor[Tuple2[String, util.SortedMap[AnyRef, AnyRef]]]], upStreamName)
            newsp = FlatMapProducer(UniqueId.incrementAndGetId(), newmapper)
            newsp.setGraph(dag)
            newsp.setConfig(config)
          }
          case _ => throw new IllegalArgumentException
        }
        // remove old StreamProducer and replace that with new StreamProducer
        val incomingEdges = dag.incomingEdgesOf(current)
        incomingEdges.foreach(e => toBeAddedEdges += StreamConnector(e.from, newsp))
        val outgoingEdges = dag.outgoingEdgesOf(current)
        outgoingEdges.foreach(e => toBeAddedEdges += StreamConnector(newsp, e.to))
        toBeRemovedVertex += current
      }
      case _: MapProducer => {
        val mapper = current.asInstanceOf[MapProducer].fn
        val newfun: (AnyRef => AnyRef) = {
          a => mapper(a) match {
            case scala.Tuple2(x1, x2) => (x1, upStreamName, x2)
            case _ => throw new IllegalArgumentException
          }
        }
        current match {
          case MapProducer(id, 2, fn) => newsp = MapProducer(UniqueId.incrementAndGetId(), 3, newfun)
          case _ => throw new IllegalArgumentException
        }
        val incomingEdges = dag.incomingEdgesOf(current)
        incomingEdges.foreach(e => toBeAddedEdges += StreamConnector(e.from, newsp))
        val outgoingEdges = dag.outgoingEdgesOf(current)
        outgoingEdges.foreach(e => toBeAddedEdges += StreamConnector(newsp, e.to))
        toBeRemovedVertex += current
      }
      case s: StreamProducer if dag.inDegreeOf(s) == 0 => {
        val fn:(AnyRef => AnyRef) = {
          n => {
            n match {
              case scala.Tuple3 => n
              case scala.Tuple2(x1,x2) => (x1,upStreamName,x2)
              case scala.Tuple1(x1) => (if(x1 == null) null else x1.hashCode(),upStreamName,x1)
              case _ => (if(n == null) null else n.hashCode(),upStreamName,n)
            }
          }
        }
        newsp = MapProducer(UniqueId.incrementAndGetId(),3,fn)
        toBeAddedEdges += StreamConnector(current,newsp)
        val outgoingEdges = dag.outgoingEdgesOf(current)
        outgoingEdges.foreach(e => toBeAddedEdges += StreamConnector(newsp,e.to))
      }
      case _ => throw new IllegalArgumentException("Only FlatMapProducer and MapProducer can be replaced before AlertStreamSink")
    }
    newsp
  }
}