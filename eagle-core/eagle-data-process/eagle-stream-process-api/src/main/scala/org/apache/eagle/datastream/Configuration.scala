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

import com.typesafe.config.{Config, _}

import scala.reflect.runtime.universe._

/**
 * @since  12/4/15
 */
case class Configuration(private var config:Config) extends Serializable{
  def get:Config = config

  def set[T<:AnyRef](key:String,value:T): Unit = {
    config = config.withValue(key,ConfigValueFactory.fromAnyRef(value))
  }

  /**
   *
   * @param key config key
   * @param default default value
   * @tparam T return type
   * @return
   */
  def get[T](key:String,default:T=null)(implicit tag:TypeTag[T]):T = {
    if(get.hasPath(key)) {
      get(key)
    } else default
  }

  def get[T](key:String)(implicit tag: TypeTag[T]):T = tag.tpe match {
    case STRING_TYPE => config.getString(key).asInstanceOf[T]
    case TypeTag.Double => get.getDouble(key).asInstanceOf[T]
    case TypeTag.Long => get.getLong(key).asInstanceOf[T]
    case TypeTag.Int => get.getInt(key).asInstanceOf[T]
    case TypeTag.Byte => get.getBytes(key).asInstanceOf[T]
    case TypeTag.Boolean => get.getBoolean(key).asInstanceOf[T]
    case NUMBER_TYPE => get.getNumber(key).asInstanceOf[T]
    case OBJECT_TYPE => get.getObject(key).asInstanceOf[T]
    case VALUE_TYPE => get.getValue(key).asInstanceOf[T]
    case ANY_REF_TYPE => get.getAnyRef(key).asInstanceOf[T]
    case INT_LIST_TYPE => get.getIntList(key).asInstanceOf[T]
    case DOUBLE_LIST_TYPE => get.getDoubleList(key).asInstanceOf[T]
    case BOOL_LIST_TYPE => get.getBooleanList(key).asInstanceOf[T]
    case LONG_LIST_TYPE => get.getLongList(key).asInstanceOf[T]
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