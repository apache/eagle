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

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.eagle.dataproc.util.ConfigOptionParser
import org.apache.eagle.datastream.core._
import org.apache.eagle.datastream.storm.StormExecutionEnvironment

import scala.reflect.runtime.universe._

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
  type storm = StormExecutionEnvironment

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
    getWithConfig[T](ConfigFactory.load())
  }

  /**
   *
   * @param config
   * @param typeTag
   * @tparam T
   * @return
   */
  def getWithConfig[T <: ExecutionEnvironment](config:Config)(implicit typeTag: TypeTag[T]): T ={
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
    getWithConfig[T](new ConfigOptionParser().load(args))
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

  def get[T<:ExecutionEnvironment](clazz:Class[T], config:Config):T ={
    get[T](config,clazz)
  }

  /**
   * Support java style
    *
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