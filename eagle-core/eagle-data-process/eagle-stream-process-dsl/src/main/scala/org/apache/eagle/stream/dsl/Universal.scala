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
package org.apache.eagle.stream.dsl

import com.typesafe.config.ConfigFactory
import org.apache.eagle.stream.dsl.builder.StreamBuilder
import org.apache.eagle.stream.dsl.definition.{SqlCode, DataStream}

import scala.reflect.runtime.{universe => ru}

object universal extends StreamBuilder{
  type storm = org.apache.eagle.datastream.storm.StormExecutionEnvironment

  // Initialize as storm environment by default
  // TODO: May need define environment from configuration or system properties
  init[storm](ConfigFactory.load())

  implicit class StringPrefix(val sc:StringContext) extends AnyVal{
    def sql(arg:Any):SqlCode = SqlCode(arg.asInstanceOf[String])

    def cf[T](arg:Any)(implicit tag: ru.TypeTag[T]):T = conf[T](arg)
    def conf[T](arg:Any)(implicit tag: ru.TypeTag[T]):T = context.getConfig.get[T](arg.asInstanceOf[String])

    def $(arg:Any):DataStream = {
      getStream(sc.parts(0))
    }
  }
}