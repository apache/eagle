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
package org.apache.eagle.stream.dsl.experimental

import scala.io.Source
import scala.reflect.runtime.{currentMirror => cm}
import scala.tools.reflect.ToolBox

object Func{
  def add(a:Int,b:Int) = {
    println(s"$a+$b = ${a+b}")
    a+b
  }
}
case class Result(a:Int,b:Int,result:Int)
object Add{
  def apply(a:Int,b:Int) = {
    println(s"Add($a,$b) = ${a+b}")
    Result(a,b,a+b)
  }
}

class DSLScriptEval{
  val sum:(Seq[Int]=>Int)= { m =>
    var s: Int = 0
    m.foreach(s += _)
    s
  }

  val code = new StringBuilder()
  val stream = classOf[DSLScriptEval].getClassLoader.getResourceAsStream("sample_code.egl")
  Source.fromInputStream(stream).getLines().foreach(line => code.append(s"$line\n"))
  println(code)
  stream.close()
  val tb = cm.mkToolBox()
  val res = tb.eval(tb.parse(
    s"""
      |// == Start ==
      |// Context Loader
      |import org.apache.eagle.stream.dsl.experimental.Func._
      |import org.apache.eagle.stream.dsl.experimental.Add
      |
      |// Code Snippet
      |$code
      |
      |// == End ==
    """.stripMargin))
  println(res)
}

object DSLScriptEval extends DSLScriptEval with App