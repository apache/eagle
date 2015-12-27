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

package org.apache.eagle.stream.dsl.interface.external

import org.apache.eagle.datastream.core.{ForeachProducer, StreamProducer}
import org.slf4j.LoggerFactory


case class ConsoleDefinition() extends Serializable{
  var level:String = "INFO"
  var stdout:Boolean = false
}

trait ConsoleAPIBuilder {
  private var _def:ConsoleDefinition = null

  def stdout:StreamProducer[Any] = ForeachProducer[Any](t=>println(t))

  def console(any: Any):StreamProducer[Any] = {
    val producer = buildConsoleStreamProducer(_def)
    _def = null
    producer
  }

  def level(flag:String = "INFO"):Unit = {
    init()
    _def.level = flag
  }

  def stdout(flag:Boolean):Unit = {
    _def.stdout = flag
  }

  private def buildConsoleStreamProducer(consoleDef:ConsoleDefinition):StreamProducer[Any] = {
    if(_def.stdout){
      ForeachProducer[Any](t=>println(t))
    }else{
      val logger = LoggerFactory.getLogger(classOf[ForeachProducer[Any]])
      _def.level match {
        case "INFO" => ForeachProducer[Any](t=>logger.info(t.toString))
        case "DEBUG" => ForeachProducer[Any](t=>logger.debug(t.toString))
        case "WARN" => ForeachProducer[Any](t=>logger.warn(t.toString))
        case _ => throw new IllegalArgumentException(s"Unsupported log level ${_def.level}")
      }
    }
  }

  private def init():Unit = {
    if(_def == null) _def = ConsoleDefinition()
  }
}

