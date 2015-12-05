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
import org.apache.eagle.dataproc.impl.storm.StormSpoutProvider
import org.apache.eagle.dataproc.util.ConfigOptionParser
import org.jgrapht.experimental.dag.DirectedAcyclicGraph
import org.slf4j.LoggerFactory

import scala.reflect.runtime.universe._

/**
 * @since 0.3.0
 */
trait ExecutionEnvironment {
  def config:Configurator

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


  def fromCollection[T](seq: Seq[T]):CollectionStreamProducer[T] = {
    val p = CollectionStreamProducer[T](seq)
    p.setup(dag,config.get)
    p
  }
}

abstract class ExecutionEnvironmentBase(private val conf:Config)  extends ExecutionEnvironment {
  private val LOG = LoggerFactory.getLogger(classOf[ExecutionEnvironmentBase])
  private val _dag = new DirectedAcyclicGraph[StreamProducer[Any], StreamConnector[Any,Any]](classOf[StreamConnector[Any,Any]])
  private val _config:Configurator = Configurator(conf)

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
  
  protected def execute(dag: StreamDAG)
}

case class StormExecutionEnvironment(private val conf:Config) extends ExecutionEnvironmentBase(conf){
  override def execute(dag: StreamDAG) : Unit = {
    StormTopologyCompiler(config.get, dag).buildTopology.execute
  }

  def from[T<:Any](source: BaseRichSpout): StormSourceProducer[T] = {
    val ret = StormSourceProducer[T](source)
    ret.config = config.get
    ret.graph = dag
    dag.addVertex(ret)
    ret
  }

  def from[T<:Any](sourceProvider: StormSpoutProvider):StormSourceProducer[T] = from(sourceProvider.getSpout(config.get))
}

/**
 * Execution environment factory
 *
 * The factory is mainly used for create or manage execution environment,
 * and also handles the shared works like configuration, arguments for execution environment
 *
 * Notice: this factory class should not know any implementation like storm or spark
 *
 * @since 0.3.0
 */
object ExecutionEnvironments{
  /**
   * Use `'''get[StormExecutionEnvironment](config)'''` instead
   *
   * @param config
   * @return
   */
  @deprecated("Execution environment should not know implementation of Storm")
  def getStorm(config : Config) = new StormExecutionEnvironment(config)

  /**
   * Use `'''get[StormExecutionEnvironment]'''` instead
   *
   * @return
   */
  @deprecated("Execution environment should not know implementation of Storm")
  def getStorm:StormExecutionEnvironment = {
    val config = ConfigFactory.load()
    getStorm(config)
  }

  /**
   * Use `'''get[StormExecutionEnvironment](args)'''` instead
   *
   * @see get[StormExecutionEnvironment](args)
   *
   * @param args
   * @return
   */
  @deprecated("Execution environment should not know implementation of Storm")
  def getStorm(args:Array[String]):StormExecutionEnvironment = {
    getStorm(new ConfigOptionParser().load(args))
  }

  /**
   * @param typeTag
   * @tparam T
   * @return
   */
  def get[T<:ExecutionEnvironment](implicit typeTag: TypeTag[T]): T ={
    get[T](ConfigFactory.load())
  }

  /**
   *
   * @param config
   * @param typeTag
   * @tparam T
   * @return
   */
  def get[T<:ExecutionEnvironment](config:Config)(implicit typeTag: TypeTag[T]): T ={
    typeTag.mirror.runtimeClass(typeOf[T]).getConstructor(classOf[Config]).newInstance(config).asInstanceOf[T]
  }

  /**
   *
   * @param args
   * @param typeTag
   * @tparam T
   * @return
   */
  def get[T<:ExecutionEnvironment](args:Array[String])(implicit typeTag: TypeTag[T]): T ={
    get[T](new ConfigOptionParser().load(args))
  }

  /**
   * Support java style for default config
   *
   * @param clazz execution environment class
   * @tparam T execution environment type
   * @return
   */
  def get[T<:ExecutionEnvironment](clazz:Class[T]):T ={
    get[T](ConfigFactory.load(),clazz)
  }

  /**
   * Support java style
   * @param config command config
   * @param clazz execution environment class
   * @tparam T execution environment type
   * @return
   */
  def get[T<:ExecutionEnvironment](config:Config,clazz:Class[T]):T ={
    clazz.getConstructor(classOf[Config]).newInstance(config)
  }

  /**
   * Support java style
   *
   * @param args command arguments in string array
   * @param clazz execution environment class
   * @tparam T execution environment type
   * @return
   */
  def get[T<:ExecutionEnvironment](args:Array[String],clazz:Class[T]):T ={
    clazz.getConstructor(classOf[Config]).newInstance(new ConfigOptionParser().load(args))
  }
}