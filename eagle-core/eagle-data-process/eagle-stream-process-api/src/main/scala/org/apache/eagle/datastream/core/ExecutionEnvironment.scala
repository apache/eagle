/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.eagle.datastream.core

import com.typesafe.config.Config
import org.apache.eagle.datastream.utils.GraphPrinter
import org.jgrapht.experimental.dag.DirectedAcyclicGraph

/**
 * @since 0.3.0
 */
trait ExecutionEnvironment {
  def config:Configuration

  /**
   * Business logic DAG
   * @return
   */
  def dag:DirectedAcyclicGraph[StreamProducer[Any], StreamConnector[Any,Any]]

  /**
   * Start to execute
   */
  def execute():Unit

  /**
   * Support Java Style Config
   *
   * @return
   */
  def getConfig:Config = config.get
}

/**
 * @todo Use Configuration instead of Config
 *
 * @param conf
 */
abstract class ExecutionEnvironmentBase(private val conf:Config)  extends ExecutionEnvironment with StreamSourceBuilder {
  implicit private val _dag = new DirectedAcyclicGraph[StreamProducer[Any], StreamConnector[Any,Any]](classOf[StreamConnector[Any,Any]])
  private val _config:Configuration = Configuration(conf)

  override def dag = _dag
  override def config = _config

  override def execute(): Unit = {
    implicit val i_conf = _config.get
    StreamNameExpansion()
    GraphPrinter.print(dag,message="Before expanded DAG ")
    StreamAnalyzeExpansion()
    GraphPrinter.print(dag,message="after analyze expanded DAG ")
    StreamAlertExpansion()
    StreamUnionExpansion()
    StreamGroupbyExpansion()
    StreamParallelismConfigExpansion()
    StreamNameExpansion()
    StreamPersistExpansion()
    GraphPrinter.print(dag,message="After expanded DAG ")

    GraphPrinter.printDotDigraph(dag)

    val streamDAG = StreamDAGTransformer.transform(dag)
    execute(streamDAG)
  }

  protected def execute(dag: StreamDAG)
}