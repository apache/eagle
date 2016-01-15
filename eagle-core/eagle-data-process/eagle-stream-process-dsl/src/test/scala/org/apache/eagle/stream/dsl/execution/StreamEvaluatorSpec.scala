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

import org.apache.eagle.stream.dsl.StreamBuilder._
import org.scalatest.{FlatSpec, Matchers}

class StreamEvaluatorSpec extends FlatSpec with Matchers{
  import StreamEvaluatorSpec._

  "stream code" should "parse successfully" in {
    val ret = StreamEvaluator(code).parse
    println(ret)
  }

  "stream code" should "compile successfully" in {
    val ret = StreamEvaluator(code).compile
    println(ret)
  }

  "stream code" should "evaluate successfully" in {
    // TODO: Fix the bug
    //
    // Failed because scalatest plugin bug for storm topology local runner due to following exception
    //
    // Exception:
    //  2015-12-27 14:51:50,788 ERROR [Thread-5] storm.event[0]: Error when processing event
    //    java.io.FileNotFoundException: /Users/${user}/Library/Application Support/IntelliJIdea14/Scala/lib/Runners.jar (No such file or directory)
    //
    intercept[Throwable] {
      val ret = StreamEvaluator(code).evaluate[storm]
      println(ret)
    }
  }
}

object StreamEvaluatorSpec{
  val code =
    """
      | "logStream" := stream from Seq(
      |      "55.3.244.1 GET /index.html 15824 0.043",
      |      "55.3.244.1 GET /index.html 15824 0.043",
      |      "55.3.244.1 GET /index.html 15824 0.043",
      |      "55.3.244.1 GET /index.html 15824 0.043",
      |      "55.3.244.1 GET /index.html 15824 0.043",
      |      "55.3.244.1 GET /index.html 15824 0.043"
      |    ) as ("line"->'string) parallism 10
      |
      |  "parserStream" := $"logStream" grok {
      |    pattern("line"->"(?<ip>\\d+\\.\\d+\\.\\d+\\.\\d+)\\s+(?<method>\\w+)\\s+(?<path>[\\w/\\.]+)\\s+(?<bytes>\\d+)\\s+(?<time>[\\d\\.]+)")
      |  }
      |
      |  $"parserStream" > stdout parallism 1
      |
    """.stripMargin
}

object StreamEvaluatorSpec_1 extends App {
  val ret = StreamEvaluator(StreamEvaluatorSpec.code).evaluate[storm]
  println(ret)
}