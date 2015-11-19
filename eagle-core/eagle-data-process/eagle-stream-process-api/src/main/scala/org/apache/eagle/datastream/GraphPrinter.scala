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
import org.slf4j.LoggerFactory
import scala.collection.JavaConversions._

object GraphPrinter {
  private val LOG = LoggerFactory.getLogger(GraphPrinter.getClass)
  def print(dag: DirectedAcyclicGraph[StreamProducer, StreamConnector]): Unit ={
    val iter = dag.iterator()
    while(iter.hasNext) {
      val current = iter.next()
      dag.outgoingEdgesOf(current).foreach(edge => {
        LOG.info(edge.from + "{" + edge.from.parallelism + "}" +" => " + edge.to + "{" + edge.to.parallelism + "}" + " with groupByFields " + edge.groupByFields)
      })
    }
  }
}
