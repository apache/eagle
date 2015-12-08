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
import org.jgrapht.experimental.dag.DirectedAcyclicGraph
import org.slf4j.LoggerFactory

/**
 * @since  12/8/15
 */
case class StreamTypeExpansion(config: Config) extends StreamDAGExpansion(config) {
  val LOG = LoggerFactory.getLogger(classOf[StreamTypeExpansion])
  override def expand(dag: DirectedAcyclicGraph[StreamProducer[Any], StreamConnector[Any, Any]]): Unit = {
    val iter = dag.iterator()
    while(iter.hasNext){
      val next = iter.next()
      if(next.typeClass == null){
        LOG.warn(s"Stream type of $next is unknown, set as ${classOf[AnyRef]} by default")
        next.typeClass = classOf[AnyRef]
      }
    }
  }
}

object StreamTypeExpansion{
  def apply()(implicit config:Config, dag: DirectedAcyclicGraph[StreamProducer[Any], StreamConnector[Any,Any]]): StreamTypeExpansion ={
    val e = StreamTypeExpansion(config)
    e.expand(dag)
    e
  }
}