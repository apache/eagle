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

import java.util

import scala.collection.JavaConversions.asScalaSet
import scala.collection.mutable.ListBuffer

import org.apache.eagle.alert.dao.AlertDefinitionDAOImpl
import org.apache.eagle.datastream.EagleTuple
import org.apache.eagle.datastream.JavaStormExecutorForAlertWrapper
import org.apache.eagle.datastream.JavaStormStreamExecutor
import org.apache.eagle.datastream.StormStreamExecutor
import org.apache.eagle.datastream.Tuple2
import org.apache.eagle.datastream.storm.StormExecutorForAlertWrapper
import org.apache.eagle.datastream.utils.AlertExecutorConsumerUtils
import org.apache.eagle.executor.AlertExecutorCreationUtils
import org.apache.eagle.service.client.EagleServiceConnector
import org.jgrapht.experimental.dag.DirectedAcyclicGraph
import org.slf4j.LoggerFactory

import com.typesafe.config.Config

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

case class StreamAlertExpansion(config: Config) extends StreamDAGExpansion(config) {
  val LOG = LoggerFactory.getLogger(classOf[StreamAlertExpansion])

  override def expand(dag: DirectedAcyclicGraph[StreamProducer[Any], StreamConnector[Any,Any]]): Unit ={
    val iter = dag.iterator()
    val toBeAddedEdges = new ListBuffer[StreamConnector[Any,Any]]
    val toBeRemovedVertex = new ListBuffer[StreamProducer[Any]]
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

  def onIteration(toBeAddedEdges: ListBuffer[StreamConnector[Any,Any]], toBeRemovedVertex: ListBuffer[StreamProducer[Any]], 
               dag: DirectedAcyclicGraph[StreamProducer[Any], StreamConnector[Any,Any]], current: StreamProducer[Any], child: StreamProducer[Any]): Unit = {
    child match {
      case AlertStreamSink(upStreamNames, alertExecutorId, withConsumer,strategy) => {
        /**
         * step 1: wrapper previous StreamProducer with one more field "streamName"
         * for AlertStreamSink, we check previous StreamProducer and replace that
         */
        val newStreamProducers = rewriteWithStreamOutputWrapper(current, dag, toBeAddedEdges, toBeRemovedVertex, upStreamNames)

        /**
         * step 2: partition alert executor by policy partitioner class
         */
        val alertExecutors = AlertExecutorCreationUtils.createAlertExecutors(config, new AlertDefinitionDAOImpl(new EagleServiceConnector(config)), upStreamNames, alertExecutorId)
        var alertProducers = new scala.collection.mutable.MutableList[StreamProducer[Any]]
        alertExecutors.foreach(exec => {
          val t = FlatMapProducer(exec).nameAs(exec.getExecutorId + "_" + exec.getPartitionSeq).initWith(dag,config, hook = false)
          alertProducers += t
          newStreamProducers.foreach(newsp => toBeAddedEdges += StreamConnector[Any,Any](newsp, t,Seq(0)))
          if (strategy == null) {
             newStreamProducers.foreach(newsp => toBeAddedEdges += StreamConnector(newsp,t,Seq(0)))
          }
          else {
            newStreamProducers.foreach(newsp => toBeAddedEdges += StreamConnector(newsp,t,strategy))
          }
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

  protected def rewriteWithStreamOutputWrapper(current: org.apache.eagle.datastream.core.StreamProducer[Any], dag: org.jgrapht.experimental.dag.DirectedAcyclicGraph[org.apache.eagle.datastream.core.StreamProducer[Any],org.apache.eagle.datastream.core.StreamConnector[Any,Any]], toBeAddedEdges: scala.collection.mutable.ListBuffer[org.apache.eagle.datastream.core.StreamConnector[Any,Any]], toBeRemovedVertex: scala.collection.mutable.ListBuffer[org.apache.eagle.datastream.core.StreamProducer[Any]], upStreamNames: java.util.List[String]) = {/**
     * step 1: wrapper previous StreamProducer with one more field "streamName"
     * for AlertStreamSink, we check previous StreamProducer and replace that
     */
    val newStreamProducers = new ListBuffer[StreamProducer[Any]]
    current match {
      case StreamUnionProducer(others) => {
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
      case _: MapperProducer[AnyRef,AnyRef] => {
        newStreamProducers += replace(toBeAddedEdges, toBeRemovedVertex, dag, current, upStreamNames.get(0))
      }
      case s: StreamProducer[AnyRef] if dag.inDegreeOf(s) == 0 => {
        newStreamProducers += replace(toBeAddedEdges, toBeRemovedVertex, dag, current, upStreamNames.get(0))
      }
      case p@_ => throw new IllegalStateException(s"$p can not be put before AlertStreamSink, only StreamUnionProducer,FlatMapProducer and MapProducer are supported")
    }
    newStreamProducers
  }

  protected def replace(toBeAddedEdges: ListBuffer[StreamConnector[Any,Any]], toBeRemovedVertex: ListBuffer[StreamProducer[Any]],
                      dag: DirectedAcyclicGraph[StreamProducer[Any], StreamConnector[Any,Any]], current: StreamProducer[Any], upStreamName: String) : StreamProducer[Any]= {
    var newsp: StreamProducer[Any] = null
    current match {
      case _: FlatMapProducer[AnyRef, AnyRef] => {
        val mapper = current.asInstanceOf[FlatMapProducer[_, _]].mapper
        mapper match {
          case a: JavaStormStreamExecutor[EagleTuple] => {
            val newmapper = new JavaStormExecutorForAlertWrapper(a.asInstanceOf[JavaStormStreamExecutor[Tuple2[String, util.SortedMap[AnyRef, AnyRef]]]], upStreamName)
            newsp = FlatMapProducer(newmapper).initWith(dag,config,hook = false)
          }
          case b: StormStreamExecutor[EagleTuple] => {
            val newmapper = StormExecutorForAlertWrapper(b.asInstanceOf[StormStreamExecutor[Tuple2[String, util.SortedMap[AnyRef, AnyRef]]]], upStreamName)
            newsp = FlatMapProducer(newmapper).initWith(dag,config,hook = false)
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
      case _: MapperProducer[Any,Any] => {
        val mapper = current.asInstanceOf[MapperProducer[Any,Any]].fn
        val newfun: (Any => Any) = {
          a => mapper(a) match {
            case scala.Tuple2(x1, x2) => (x1, upStreamName, x2)
            case _ => throw new IllegalArgumentException
          }
        }
        current match {
          case MapperProducer(2, fn) => newsp = MapperProducer(3, newfun)
          case _ => throw new IllegalArgumentException
        }
        val incomingEdges = dag.incomingEdgesOf(current)
        incomingEdges.foreach(e => toBeAddedEdges += StreamConnector(e.from, newsp))
        val outgoingEdges = dag.outgoingEdgesOf(current)
        outgoingEdges.foreach(e => toBeAddedEdges += StreamConnector(newsp, e.to))
        toBeRemovedVertex += current
      }
      case s: StreamProducer[Any] if dag.inDegreeOf(s) == 0 => {
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
        newsp = MapperProducer(3,fn)
        toBeAddedEdges += StreamConnector(current,newsp)
        val outgoingEdges = dag.outgoingEdgesOf(current)
        outgoingEdges.foreach(e => toBeAddedEdges += StreamConnector(newsp,e.to))
      }
      case _ => throw new IllegalArgumentException("Only FlatMapProducer and MapProducer can be replaced before AlertStreamSink")
    }
    newsp
  }
}

object StreamAlertExpansion{
  def apply()(implicit config:Config, dag: DirectedAcyclicGraph[StreamProducer[Any], StreamConnector[Any,Any]]): StreamAlertExpansion ={
    val e = StreamAlertExpansion(config)
    e.expand(dag)
    e
  }
}


