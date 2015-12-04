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
package org.apache.eagle.datastream

import backtype.storm.topology.base.BaseRichSpout
import com.typesafe.config.{ConfigFactory, Config}
import org.apache.eagle.dataproc.impl.storm.StormSpoutProvider
import org.apache.eagle.dataproc.util.ConfigOptionParser
import org.jgrapht.experimental.dag.DirectedAcyclicGraph
import org.slf4j.LoggerFactory


trait ExecutionEnvironment{

  def config:ConfigWrapper

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


abstract class BaseExecutionEnvironment(private val conf:Config)  extends ExecutionEnvironment {
  private val LOG = LoggerFactory.getLogger(classOf[BaseExecutionEnvironment])
  private val _dag = new DirectedAcyclicGraph[StreamProducer[Any], StreamConnector[Any,Any]](classOf[StreamConnector[Any,Any]])
  private val _config:ConfigWrapper = ConfigWrapper(conf)

  override def dag = _dag
  override def config = _config

  override def execute(): Unit ={
    LOG.info("initial graph:\n")
    GraphPrinter.print(dag)
    new StreamAlertExpansion(config.get).expand(dag)
    LOG.info("after StreamAlertExpansion graph:")
    GraphPrinter.print(dag)
    new StreamUnionExpansion(config.get).expand(dag)
    LOG.info("after StreamUnionExpansion graph:")
    GraphPrinter.print(dag)
    new StreamGroupbyExpansion(config.get).expand(dag)
    LOG.info("after StreamGroupbyExpansion graph:")
    GraphPrinter.print(dag)
    new StreamNameExpansion(config.get).expand(dag)
    LOG.info("after StreamNameExpansion graph:")
    GraphPrinter.print(dag)
    new StreamParallelismConfigExpansion(config.get).expand(dag)
    LOG.info("after StreamParallelismConfigExpansion graph:")
    GraphPrinter.print(dag)
    val streamDAG = StreamDAGTransformer.transform(dag)
    execute(streamDAG)
  }
  
  def execute(dag: StreamDAG)
}

case class StormExecutionEnvironment(private val conf:Config) extends BaseExecutionEnvironment(conf){
  override def execute(dag: StreamDAG) : Unit = {
    StormTopologyCompiler(config.get, dag).buildTopology.execute
  }

  def from[T](source: BaseRichSpout): StormSourceProducer[T] = {
    val ret = StormSourceProducer[T](source)
    ret.config = config.get
    ret.graph = dag
    dag.addVertex(ret)
    ret
  }

  def from[T](sourceProvider: StormSpoutProvider):StormSourceProducer[T] = from(sourceProvider.getSpout(config.get))
}

/**
 * Execution Environment should not know any implementation layer type
 *
 */
object ExecutionEnvironments{
  def getStorm(config : Config) = new StormExecutionEnvironment(config)

  def getStorm:StormExecutionEnvironment = {
    val config = ConfigFactory.load()
    getStorm(config)
  }

  def getStorm(args:Array[String]):StormExecutionEnvironment = {
    getStorm(new ConfigOptionParser().load(args))
  }
}