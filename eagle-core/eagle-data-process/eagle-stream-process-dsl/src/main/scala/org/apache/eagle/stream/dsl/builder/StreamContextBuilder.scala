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
package org.apache.eagle.stream.dsl.builder

import com.typesafe.config.Config
import org.apache.eagle.datastream.ExecutionEnvironments
import org.apache.eagle.datastream.core.ExecutionEnvironment
import org.apache.eagle.stream.dsl.definition.{DataStream, StreamContext}
import org.slf4j.LoggerFactory

import scala.reflect.runtime.{universe => ru}

trait StreamContextBuilder extends Serializable{
  private val logger = LoggerFactory.getLogger(classOf[StreamContextBuilder])
  private var _context:StreamContext = null
  def context(context:StreamContext):Unit = {
    if(_context!=null && logger.isDebugEnabled) logger.debug(s"Initializing with $context")
    _context = context
  }

  def context:StreamContext = {
    if(_context ==null) throw new IllegalStateException("Context is not initialized")
    _context
  }

  protected def getStream(name:String) = context.getStreamManager.getStream(name)
  protected def setStream(stream:DataStream) = context.getStreamManager.setStream(stream)

  /**
   * Override App#args:Array[String] method
   *
   * @return
   */
  def init[T<:ExecutionEnvironment](args:Array[String] = Array[String]())(implicit typeTag: ru.TypeTag[T]) = {
    context(StreamContext(ExecutionEnvironments.get[T](args)))
  }

  def init[T<:ExecutionEnvironment](config:Config)(implicit typeTag: ru.TypeTag[T]) = {
    context(StreamContext(ExecutionEnvironments.get[T](config)))
  }

  def init[T<:ExecutionEnvironment](config:Config,executionEnvironment:Class[T]) = {
    context(StreamContext(ExecutionEnvironments.get[T](config,executionEnvironment)))
  }

  def submit:ExecutionEnvironment = {
    context.getEnvironment.execute()
    context.getEnvironment
  }
}
