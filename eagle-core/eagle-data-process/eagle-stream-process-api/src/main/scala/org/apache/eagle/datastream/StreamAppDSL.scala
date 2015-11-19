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
import com.typesafe.config._
import org.apache.eagle.dataproc.impl.storm.AbstractStormSpoutProvider
import org.apache.eagle.dataproc.util.ConfigOptionParser

import scala.reflect.runtime.universe._

/**
 * @since  11/6/15
 */
trait ConfigContext{
  def set(config:Config)
  def config:Config

  def set[T<:AnyRef](key:String,value:T): Unit = {
    set(config.withValue(key,ConfigValueFactory.fromAnyRef(value)))
  }

  /**
   *
   * @param key config key
   * @param default default value
   * @tparam T return type
   * @return
   */
  def get[T](key:String,default:T=null)(implicit tag:TypeTag[T]):T = {
    if(config.hasPath(key)) {
      get(key)
    } else default
  }

  def get[T](key:String)(implicit tag: TypeTag[T]):T = tag.tpe match {
    case STRING_TYPE => config.getString(key).asInstanceOf[T]
    case TypeTag.Double => config.getDouble(key).asInstanceOf[T]
    case TypeTag.Long => config.getLong(key).asInstanceOf[T]
    case TypeTag.Int => config.getInt(key).asInstanceOf[T]
    case TypeTag.Byte => config.getBytes(key).asInstanceOf[T]
    case TypeTag.Boolean => config.getBoolean(key).asInstanceOf[T]
    case NUMBER_TYPE => config.getNumber(key).asInstanceOf[T]
    case OBJECT_TYPE => config.getObject(key).asInstanceOf[T]
    case VALUE_TYPE => config.getValue(key).asInstanceOf[T]
    case ANY_REF_TYPE => config.getAnyRef(key).asInstanceOf[T]
    case INT_LIST_TYPE => config.getIntList(key).asInstanceOf[T]
    case DOUBLE_LIST_TYPE => config.getDoubleList(key).asInstanceOf[T]
    case BOOL_LIST_TYPE => config.getBooleanList(key).asInstanceOf[T]
    case LONG_LIST_TYPE => config.getLongList(key).asInstanceOf[T]
    case _ => throw new UnsupportedOperationException(s"$tag is not supported yet")
  }

  val STRING_TYPE = typeOf[String]
  val NUMBER_TYPE = typeOf[Number]
  val INT_LIST_TYPE = typeOf[List[Int]]
  val BOOL_LIST_TYPE = typeOf[List[Boolean]]
  val DOUBLE_LIST_TYPE = typeOf[List[Double]]
  val LONG_LIST_TYPE = typeOf[List[Double]]
  val OBJECT_TYPE = typeOf[ConfigObject]
  val VALUE_TYPE = typeOf[ConfigValue]
  val ANY_REF_TYPE = typeOf[AnyRef]
}

/**
 * Stream APP DSL
 * @tparam E
 */
trait StreamApp[+E<:ExecutionEnvironment] extends App with ConfigContext{
  private var _executed = false
  private var _config:Config = null

  override def config:Config = _config

  override def set(config:Config) = _config = config

  def env:E

  def execute() {
    env.execute()
    _executed = true
  }

  override def main(args: Array[String]): Unit = {
    _config = new ConfigOptionParser().load(args)
    super.main(args)
    if(!_executed) execute()
  }
}

trait StormStreamApp extends StreamApp[StormExecutionEnvironment]{
  private var _env:StormExecutionEnvironment = null
  def source(sourceProvider: AbstractStormSpoutProvider) = {
    val spout = sourceProvider.getSpout(config)
    env.newSource(spout)
  }

  def source(spout:BaseRichSpout) = env.newSource(spout)

  override def env:StormExecutionEnvironment = {
    if(_env == null){
      _env = ExecutionEnvironmentFactory.getStorm(config)
    }
    _env
  }
}