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
package org.apache.eagle.stream.dsl.execution

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.eagle.datastream.core.ExecutionEnvironment
import org.apache.eagle.stream.dsl.universal._
import org.slf4j.LoggerFactory

import scala.reflect.runtime.universe._
import scala.reflect.runtime.{currentMirror => cm}
import scala.tools.reflect.ToolBox

case class ParseException(message:String,throwable:Throwable) extends Exception(message,throwable)
case class CompileException(message:String,throwable:Throwable) extends Exception(message,throwable)
case class EvaluateException(message:String,throwable:Throwable) extends Exception(message,throwable)

case class StreamRuntime(code:String,config:Config = ConfigFactory.load()) {
  private val logger = LoggerFactory.getLogger(classOf[StreamRuntime])
  val tb = cm.mkToolBox()

  def format:String = StreamFormatter(config).format(code)

  @throws[ParseException]
  def parse = {
    val formatted = format
    if(logger.isDebugEnabled)
      logger.debug(s"Parsing \n $formatted")
    else
      logger.info("Parsing ...")
    try {
      val ret = tb.parse(format)
      if (logger.isDebugEnabled) {
        logger.debug(s"Successfully parsed as\n $ret")
      }else{
        logger.info("Successfully parsed")
      }
      ret
    } catch {
      case e:Throwable => {
        logger.error(s"Failed to parse $formatted",e)
        throw ParseException(s"Failed to parse $formatted",e)
      }
    }
  }

  @throws[CompileException]
  def compile:()=>Any = {
    val tree = parse
    if(logger.isDebugEnabled)
      logger.debug(s"Compiling $tree")
    else logger.info("Compiling")
    try {
      tb.compile(tree)
    }catch{
      case e:Throwable =>{
        sys.error(s"Failed to compile $tree\nException: $e")
        throw CompileException(s"Failed to compile $tree",e)
      }
    }
  }

  @throws[EvaluateException]
  def evaluate(environmentClass:Class[_ <:ExecutionEnvironment]=classOf[storm]):Any = {
    val tree = parse
    if(logger.isDebugEnabled)
      logger.debug(s"Evaluating $tree")
    else
      logger.info("Evaluating")
    try {
      init(config,environmentClass)
      tb.eval(tree)
      submit
    } catch {
      case e:Throwable =>{
        sys.error(s"Failed to evaluate $tree\nException: $e")
        throw EvaluateException(s"Failed to evaluate $tree",e)
      }
    }
  }

  @throws[EvaluateException]
  def evaluate[T<:ExecutionEnvironment](implicit typeTag:TypeTag[T]):Any = {
    val tree = parse
    if(logger.isDebugEnabled) logger.debug(s"Evaluating $tree")
    try {
      init[T](config)
      tb.eval(tree)
      submit
    } catch {
      case e:Throwable => {
        sys.error(s"Failed to evaluate $tree\nException: $e")
        throw EvaluateException(s"Failed to evaluate $tree",e)
      }
    }
  }
}

object StreamRuntime {
  def main(args:Array[String]): Unit ={
    // stream.app.conf
  }
}