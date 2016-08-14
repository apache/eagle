/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.eagle.dataproc.util.ConfigOptionParser
import org.jgrapht.experimental.dag.DirectedAcyclicGraph

import scala.reflect
import scala.reflect.runtime.universe._

trait StreamContextBuilder extends StreamSourceBuilder {
  def config:Configuration
  /**
   * Business logic DAG
    *
    * @return
   */
  def dag:DirectedAcyclicGraph[StreamProducer[Any], StreamConnector[Any,Any]]
  /**
   * Support Java Style Config
   *
   * @return
   */
  def getConfig:Config = config.get
  def build:StreamDAG
  def submit[E<:ExecutionEnvironment](implicit typeTag:TypeTag[E]):Unit
  def submit(env:ExecutionEnvironment):Unit
  def submit(clazz:Class[ExecutionEnvironment]):Unit
}

class StreamContext(private val conf:Config) extends StreamContextBuilder{
  implicit private val _dag = new DirectedAcyclicGraph[StreamProducer[Any], StreamConnector[Any,Any]](classOf[StreamConnector[Any,Any]])
  private val _config:Configuration = Configuration(conf)
  override def dag = _dag
  override def config = _config

  def build:StreamDAG = {
    null
  }


  override def submit(env: ExecutionEnvironment): Unit = {
    env.submit(this)
  }

  def submit[E<:ExecutionEnvironment](implicit typeTag:TypeTag[E]):Unit = {

  }


  override def submit(clazz: Class[ExecutionEnvironment]): Unit = {

  }
}

object StreamContext {
  /**
   * @return
   */
  def apply():StreamContext = {
    new StreamContext(ConfigFactory.load())
  }

  /**
   *
   * @param args
   * @return
   */
  def apply(args:Array[String]):StreamContext ={
    new StreamContext(new ConfigOptionParser().load(args))
  }
}