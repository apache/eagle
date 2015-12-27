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

import org.slf4j.LoggerFactory

import scala.reflect.runtime.{currentMirror => cm}
import scala.tools.reflect.ToolBox

case class StreamEvaluator(code:String) {
  private val logger = LoggerFactory.getLogger(classOf[StreamEvaluator])
  val tb = cm.mkToolBox()

  def format:String =
    s"""
      | import org.apache.eagle.stream.dsl.StreamApp._
      |
      | init[storm](Array[String]())
      |
      | $code
      |
      | submit
    """.stripMargin

  @throws[Exception]
  def parse = {
    val formatted = format
    if(logger.isDebugEnabled) logger.debug(s"Parsing \n $formatted")
    val ret = tb.parse(format)
    if(logger.isDebugEnabled) logger.debug(s"Parsed as\n $ret")
    ret
  }

  @throws[Exception]
  def compile:()=>Any = {
    val tree = parse
    if(logger.isDebugEnabled) logger.debug(s"Compiling $tree")
    tb.compile(tree)
  }

  @throws[Exception]
  def evaluate:Any = {
    val tree = parse
    if(logger.isDebugEnabled) logger.debug(s"Evaluating $tree")
    tb.eval(tree)
  }
}

object StreamEvaluator {
  def main(args:Array[String]): Unit ={
    // stream.app.conf

  }
}