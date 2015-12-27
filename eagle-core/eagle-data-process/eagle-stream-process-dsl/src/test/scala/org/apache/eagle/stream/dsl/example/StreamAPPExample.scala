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

import org.apache.eagle.stream.dsl.StreamApp._

/**
 * Simplest example
 */
object StreamAPPExample_1 extends App {
  init[storm](args)

  define("number") from Range(1,10000)

  "number" ~> stdout parallism 1

  submit
}

object StreamAPPExample_2 extends App {
  init[storm](args)

  define("number") as ("value" -> 'integer) from Range(1,10000) parallism 1

  filter("number") where {_.as[Int] % 2 == 0}

  "number" groupBy "value" to console() parallism 1

  submit
}

object StreamAPPExample_3 extends App {
  init[storm](args)

  define("number") from Range(1,10000) parallism 1 as ("value" -> 'integer)

  filter("number") where {_.as[Int] % 2 == 0} by {(a,c)=>
    c.collect(("key",a))
  } as ("key" ->'string,"value"->'integer)

  "number" groupBy "key" to stdout parallism 1

  submit
}

object StreamAPPExample_4 extends App {
  init[storm](args)

  define("metricStream") from kafka parallism 1

  'metricStream to alert("metricExecutor")

  submit
}

object StreamAPPExample_5 extends App {
  init[storm](args)

  define("logStream") from Seq(
    "55.3.244.1 GET /index.html 15824 0.043",
    "55.3.244.1 GET /index.html 15824 0.043",
    "55.3.244.1 GET /index.html 15824 0.043",
    "55.3.244.1 GET /index.html 15824 0.043",
    "55.3.244.1 GET /index.html 15824 0.043",
    "55.3.244.1 GET /index.html 15824 0.043"
  ) as ("line"->'string) parallism 1

  filter("logStream") by grok {
    pattern("line"->"""(?<ip>\d+\.\d+\.\d+\.\d+)\s+(?<method>\w+)\s+(?<path>[\w/\.]+)\s+(?<bytes>\d+)\s+(?<time>[\d\.]+)""".r)
  } parallism 1

  'logStream to stdout

  submit
}

//object StreamAPPExample_5 extends App{
//  init[storm](args)
//
//  define("metricStream") from kafka parallism 1
//
//  alert("metricStream" -> "alertStream") by sql"""
//    from metricStream[metric=="RpcActivityForPort50020.RpcQueueTimeNumOps" and value>100] select * insert into alertStream;
//  """
//
//  aggregate("metricStream" -> "aggregatedStream") by sql"""
//   from metricStream[component=='dn' and metric=="RpcActivityForPort50020.RpcQueueTimeNumOps"].time[3600]
//    select sum(value) group by host output every 1 hour insert into aggregatedStream;
//  """ as("host" -> 'string,"metric"->'string,"sum"->'double,"timestamp"->'long)
//
//  submit
//}
//
//object StreamAPPExample_6 extends App {
//  init[storm](args)
//
//  define("logStream") from Seq("2015-12-25 18:36:06,047 INFO [http-bio-38080-exec-7] generic.ListQueryResource[264]: Output: ALL")
//  filter("logStream") by grok {
//    pattern("field"-> """""".r)
//    add_field("new_field"->"value")
//  }
//  "logStream" to stdout
//
//  submit
//}
