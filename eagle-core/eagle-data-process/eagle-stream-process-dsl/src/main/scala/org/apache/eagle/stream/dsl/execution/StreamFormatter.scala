package org.apache.eagle.stream.dsl.execution

import com.typesafe.config.Config

import scala.collection.mutable

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
case class StreamFormatter(config:Config) {
  import StreamFormatter._

  val library = mutable.ArrayBuffer(
    "org.apache.eagle.stream.dsl.universal"
  )

  def format(code:String):String = {
    val result = mutable.ArrayBuffer[String]("\n")
    result += buildImport
    result += buildCode(code)
    result.mkString("\n")
  }

  private def buildImport:String = {
    val result = mutable.ArrayBuffer[String]("\n")
    library.foreach({ library =>
      result += s"$IMPORT_PREFIX $library$IMPORT_POSTFIX"
    })
    result.mkString(NEW_LINE)
  }

  private def buildCode(code:String):String = {
    code
  }
}

object StreamFormatter {
  val IMPORT_PREFIX = "import"
  val IMPORT_POSTFIX = "._"
  val NEW_LINE = "\n"
}