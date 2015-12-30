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
package org.apache.eagle.stream.dsl.interface

import com.typesafe.config.Config
import org.apache.eagle.datastream.ExecutionEnvironments
import org.apache.eagle.datastream.core.ExecutionEnvironment
import org.apache.eagle.stream.dsl.definition.{StreamContext, StreamDefinition}

import scala.collection.mutable.ArrayBuffer
import scala.reflect.runtime.universe._


trait AbstractAPIBuilder extends APIBuilderHelper with APILifecyleListener{
  private var _context:StreamContext = null

  implicit protected var primaryStream:StreamDefinition  = null

  onInit {
    _context = null
    primaryStream = null
  }

  def context(context:StreamContext):Unit = {
    if(_context!=null) throw new IllegalStateException("Context has already been initialized")
    _context = context
  }

  def context:StreamContext = {
    if(_context ==null) throw new IllegalStateException("Context is not initialized yet")
    _context
  }

  /**
   * Override App#args:Array[String] method
   *
   * @return
   */
  def init[T<:ExecutionEnvironment](args:Array[String] = Array[String]())(implicit typeTag: TypeTag[T]) = {
    reset()
    context(StreamContext(ExecutionEnvironments.get[T](args)))
  }

  def init[T<:ExecutionEnvironment](config:Config)(implicit typeTag: TypeTag[T]) = {
    reset()
    context(StreamContext(ExecutionEnvironments.get[T](config)))
  }

  def init[T<:ExecutionEnvironment](config:Config,executionEnvironment:Class[T]) = {
    reset()
    context(StreamContext(ExecutionEnvironments.get[T](config,executionEnvironment)))
  }

  def submit:ExecutionEnvironment = {
    context.getEnvironment.execute()
    context.getEnvironment
  }

  def reset():Unit = {
    initListeners.foreach(_.apply())
  }
}

trait APIListener extends (()=>Unit) with Serializable
trait InitialListener extends APIListener
trait APILifecyleListener{
  private var _listeners = ArrayBuffer[APIListener]()

  def register(lisenter:APIListener):Unit = {
    _listeners += lisenter
  }

  protected def onInit(listener: => Unit):Unit = {
    register(new InitialListener {
      override def apply(): Unit = {
        listener
      }
    })
  }

  protected  def initListeners = _listeners.filter(_.isInstanceOf[InitialListener])
}

trait APIBuilderHelper{
  def shouldNotBeNull(value:AnyRef):Unit = {
    if(value == null) throw new NullPointerException(s"$value should not be null")
  }
}