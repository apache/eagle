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

  submit()
}

object StreamAPPExample_2 extends App {
  init[storm](args)

  define("number") as ("value" -> 'integer) from Range(1,10000) parallism 1

  filter("number") where {_.as[Int] % 2 == 0}

  "number" groupBy "value" to stdout parallism 1

  submit()
}

object StreamAPPExample_3 extends App {
  init[storm](args)

  define("number") from Range(1,10000) parallism 1 as ("value" -> 'integer)

  filter("number") where {_.as[Int] % 2 == 0} by {(a,c)=>
    c.collect(("key",a))
  } as ("key" ->'string,"value"->'integer)

  "number" groupBy "key" to stdout parallism 1

  submit()
}
