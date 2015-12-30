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
package org.apache.eagle.stream.dsl.example

import org.apache.eagle.stream.dsl.universal._

object StreamAPIExample_1 extends App {
  init[storm](args)

  // =====================================
  "stream" := stream from Range(1,10000) parallism 1 as ("value"->'integer)
  "stream" :=> stdout
  // =====================================

  submit
}

object StreamAPIExample extends App {
  init[storm](args)

  // =====================================
  "stream"            := stream from Range(1,10000) parallism 1 as ("value"->'integer)
  "filteredStream"    := $"stream" filter {_.asInstanceOf[Int] % 2 == 0}
  "filteredStream"    :=> stdout
  // =====================================

  submit
}

object StreamAPIExample_3 extends App {
  init[storm](args)

  // =====================================
  "sequenceStream"    := stream from Range(1,10000) parallism 1 as ("value"->'integer)
  "filteredStream"    := $"sequenceStream" ? {_.asInstanceOf[Int] % 2 == 0}
  "transformedStream" := $"filteredStream" | {(value,collector)=> collector.collect(("key",value))}
  "transformedStream" :=> stdout
  // =====================================

  submit
}

object StreamAPIExample_4 extends App {
  init[storm](args)

  // =====================================
  "sequenceStream"    := stream from Range(1,10000) parallism 1 as ("value"->'integer)
  "filteredStream"    := $"sequenceStream" ? {_.asInstanceOf[Int] % 2 == 0}
  "transformedStream" := $"filteredStream" | {(value,collector)=> collector.collect(("key",value))}
  "transformedStream" :=> stdout
  // =====================================

  submit
}

object StreamAPIExample_5 extends App{
  init[storm](args)

  // =====================================
  "stream" := stream from Range(1,10000) parallism 1 as ("value"->'integer)
  $"stream" ? {_.asInstanceOf[Int] % 2 == 0} | {(v,c)=> c.collect("key",v)} | stdout
  // =====================================

  submit
}

object StreamAPIExample_6 extends App{

  // =====================================
  "stream_1" := { stream from Range(1,10000) parallism 1 as ("value"->'integer) } ? {_.asInstanceOf[Int] % 1 == 0}
  "stream_2" := { stream from Range(1,20000) parallism 1 as ("value"->'integer) } ? {_.asInstanceOf[Int] % 2 == 0}
  "stream_3" := $"stream_1" union $"stream_2"
  "stream_3" :=> stdout
  // =====================================

  submit
}

object StreamAPIExample_7 extends App{

  // =====================================
  "stream" := stream from Range(1,1000)
  $"stream" alert "alertExecutor"
  // =====================================

  submit
}


object StreamAPIExample_8 extends App{

  "logStream" := stream from Seq(
    "55.3.244.1 GET /index.html 15824 0.043",
    "55.3.244.1 GET /index.html 15824 0.043",
    "55.3.244.1 GET /index.html 15824 0.043",
    "55.3.244.1 GET /index.html 15824 0.043",
    "55.3.244.1 GET /index.html 15824 0.043",
    "55.3.244.1 GET /index.html 15824 0.043"
  ) as ("line"->'string) parallism 1

//  "parserStream" := $"logStream" transform by grok {
//    pattern("line"->"""(?<ip>\d+\.\d+\.\d+\.\d+)\s+(?<method>\w+)\s+(?<path>[\w/\.]+)\s+(?<bytes>\d+)\s+(?<time>[\d\.]+)""".r)
//  } parallism 1

  "parserStream" :=> stdout

  submit
}