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
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.eagle.dataproc.impl.storm.AbstractStormSpoutProvider
import org.jgrapht.experimental.dag.DirectedAcyclicGraph
import org.slf4j.LoggerFactory

object ExecutionEnvironmentFactory{

  def getStorm(config : Config) = new StormExecutionEnvironment(config)
  def getStorm:StormExecutionEnvironment = {
    val config = ConfigFactory.load()
    getStorm(config)
  }
}

abstract class ExecutionEnvironment(config : Config){
  def execute()
}

class StormExecutionEnvironment(config: Config) extends ExecutionEnvironment(config){
  val LOG = LoggerFactory.getLogger(classOf[StormExecutionEnvironment])
  val dag = new DirectedAcyclicGraph[StreamProducer, StreamConnector](classOf[StreamConnector])

  override def execute() : Unit = {
    LOG.info("initial graph:\n")
    GraphPrinter.print(dag)
    new StreamAlertExpansion(config).expand(dag)
    LOG.info("after StreamAlertExpansion graph:")
    GraphPrinter.print(dag)
    new StreamUnionExpansion(config).expand(dag)
    LOG.info("after StreamUnionExpansion graph:")
    GraphPrinter.print(dag)
    new StreamGroupbyExpansion(config).expand(dag)
    LOG.info("after StreamGroupbyExpansion graph:")
    GraphPrinter.print(dag)
    new StreamNameExpansion(config).expand(dag)
    LOG.info("after StreamNameExpansion graph:")
    GraphPrinter.print(dag)
    new StreamParallelismConfigExpansion(config).expand(dag)
    LOG.info("after StreamParallelismConfigExpansion graph:")
    GraphPrinter.print(dag)
    val stormDag = StormStreamDAGTransformer.transform(dag)
    StormTopologyCompiler(config, stormDag).buildTopology.execute
  }

  def newSource(source: BaseRichSpout): StormSourceProducer ={
    val ret = StormSourceProducer(UniqueId.incrementAndGetId(), source)
    ret.config = config
    ret.graph = dag
    dag.addVertex(ret)
    ret
  }

  def newSource(sourceProvider: AbstractStormSpoutProvider):StormSourceProducer = newSource(sourceProvider.getSpout(config))
}