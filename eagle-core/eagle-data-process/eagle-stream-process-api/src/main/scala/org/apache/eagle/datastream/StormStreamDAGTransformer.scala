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
import scala.collection.mutable

/**
 * convert generic DAG data structure to Storm specific DAG data structure for easy topology compiler
 */
object StormStreamDAGTransformer {
  /**
   * Transform DirectedAcyclicGraph[StreamProducer, StreamConnector] into StormStreamDAG
   *
   * @param dag DirectedAcyclicGraph[StreamProducer, StreamConnector]
   * @return StormStreamDAG
   */
  def transform(dag: DirectedAcyclicGraph[StreamProducer, StreamConnector]) : StormStreamDAG = {
    val stormDAG = new StormStreamDAG(dag)
    val nodeMap = mutable.HashMap[String, StreamProducer]()
    val iter = dag.iterator()
    while(iter.hasNext){
      val sp = iter.next()
      nodeMap.put(sp.name, sp)
    }
    stormDAG.setNodeMap(nodeMap)
    stormDAG
  }
}
