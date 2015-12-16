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

package org.apache.eagle.datastream.utils

import org.apache.eagle.datastream.core.{StreamConnector, StreamProducer}
import org.jgrapht.experimental.dag.DirectedAcyclicGraph
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer

/**
 * @see scala.tools.nsc.DotDiagramGenerator
 */
object GraphPrinter {
  private val LOG = LoggerFactory.getLogger(GraphPrinter.getClass)
  def print(dag: DirectedAcyclicGraph[StreamProducer[Any], StreamConnector[Any, Any]], message:String = "Print stream DAG graph"): Unit = {
    val graphStr = ListBuffer[String]()
    val iter = dag.iterator()
    while (iter.hasNext) {
      val current = iter.next()
      dag.outgoingEdgesOf(current).foreach(edge => {
        graphStr += s"${edge.from.name}{${edge.from.parallelism}} ~> ${edge.to.name}{${edge.to.parallelism}} in ${edge.toString}"
      })
    }
    LOG.info(message+"\n{ \n\t" + graphStr.mkString("\n\t") + "\n}")
  }
  
  def printDotDigraph(dag: DirectedAcyclicGraph[StreamProducer[Any], StreamConnector[Any, Any]], title:String = "dag", message:String = "Print DOT digraph (copy and visualize with http://www.webgraphviz.com/)"): String = {
    val graphStr = ListBuffer[String]()
    val iter = dag.iterator()
    while (iter.hasNext) {
      val current = iter.next()
      dag.outgoingEdgesOf(current).foreach(edge => {
        graphStr += s""""${edge.from.name} x ${edge.from.parallelismNum}" -> "${edge.to.name} x ${edge.from.parallelismNum}" [label = "$edge"];"""
      })
    }
    val dotDigraph = s"""digraph $title { \n\t${graphStr.mkString("\n\t")} \n}"""
    LOG.info(s"""$message\n\n$dotDigraph\n""")
    dotDigraph
  }
}