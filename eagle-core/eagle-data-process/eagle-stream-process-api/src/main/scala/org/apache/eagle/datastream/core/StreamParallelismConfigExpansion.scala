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

import java.util.regex.Pattern

import com.typesafe.config.{Config, ConfigObject}
import org.jgrapht.experimental.dag.DirectedAcyclicGraph
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._

case class StreamParallelismConfigExpansion(config: Config) extends StreamDAGExpansion(config){
  val LOG = LoggerFactory.getLogger(classOf[StreamParallelismConfigExpansion])

  override def expand(dag: DirectedAcyclicGraph[StreamProducer[Any], StreamConnector[Any,Any]]) = {
    val map = getParallelismMap(config)
    val iter = dag.iterator()
    while(iter.hasNext){
      val streamProducer = iter.next()
      if(streamProducer.name != null) {
        map.foreach(tuple => {
          tuple._1.matcher(streamProducer.name).find() match {
            case true => streamProducer.parallelism(tuple._2)
            case false =>
          }
        })
      }
    }
  }

  private def getParallelismMap(config: Config) : Map[Pattern, Int]= {
    if(config.hasPath("envContextConfig.parallelismConfig")) {
      val parallelismConfig: ConfigObject = config.getObject("envContextConfig.parallelismConfig")
      parallelismConfig.asScala.toMap map {
        case (name, value) => (Pattern.compile(name), Integer.parseInt(value.unwrapped().toString))
      }
    }else{
      Map[Pattern,Int]()
    }
  }
}

object StreamParallelismConfigExpansion{
  def apply()(implicit config:Config, dag: DirectedAcyclicGraph[StreamProducer[Any], StreamConnector[Any,Any]]): StreamParallelismConfigExpansion ={
    val e = StreamParallelismConfigExpansion(config)
    e.expand(dag)
    e
  }
}