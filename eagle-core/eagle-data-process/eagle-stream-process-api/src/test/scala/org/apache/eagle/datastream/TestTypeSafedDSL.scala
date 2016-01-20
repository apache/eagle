/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.eagle.datastream

import org.apache.eagle.datastream.core.StreamContext
import org.apache.eagle.datastream.storm.StormExecutionEnvironment

/**
 * @since  12/4/15
 */
case class Entity(name:String,value:Double,var inc:Int=0)

object TestIterableWithGroupBy extends App {

  val env = ExecutionEnvironments.get[StormExecutionEnvironment](args)

  val tuples = Seq(
    Entity("a", 1),
    Entity("a", 2),
    Entity("a", 3),
    Entity("b", 2),
    Entity("c", 3),
    Entity("d", 3)
  )

  env.from(tuples)
    .groupByKey(_.name)
    .map(o => {o.inc += 2;o})
    .filter(_.name != "b")
    .filter(_.name != "c")
    .groupByKey(o=>(o.name,o.value))
    .map(o => (o.name,o))
    .map(o => (o._1,o._2.value,o._2.inc))
    .foreach(println)

  env.execute()
}

object TestIterableWithGroupByWithStreamContext extends App {
  val stream = StreamContext(args)

  val tuples = Seq(
    Entity("a", 1),
    Entity("a", 2),
    Entity("a", 3),
    Entity("b", 2),
    Entity("c", 3),
    Entity("d", 3)
  )

  stream.from(tuples)
    .groupByKey(_.name)
    .map(o => {o.inc += 2;o})
    .filter(_.name != "b")
    .filter(_.name != "c")
    .groupByKey(o=>(o.name,o.value))
    .map(o => (o.name,o))
    .map(o => (o._1,o._2.value,o._2.inc))
    .foreach(println)

  stream.submit[StormExecutionEnvironment]
}

object TestIterableWithGroupByCircularly extends App{
  val env = ExecutionEnvironments.get[StormExecutionEnvironment](args)

  val tuples = Seq(
    Entity("a", 1),
    Entity("a", 2),
    Entity("a", 3),
    Entity("b", 2),
    Entity("c", 3),
    Entity("d", 3)
  )

  env.from(tuples,recycle = true)
    .map(o => {o.inc += 2;o})
    .groupByKey(_.name)
    .foreach(println)
  env.execute()
}

object TestGroupByKeyOnSpoutproxy extends App{
  val env = ExecutionEnvironments.get[StormExecutionEnvironment](args)

  val tuples = Seq(
    Entity("a", 1),
    Entity("a", 2),
    Entity("a", 3),
    Entity("b", 2),
    Entity("c", 3),
    Entity("d", 3)
  )

  env.fromSpout[String](TestSpout())
    .groupByKey(_.charAt(0))
    .foreach(println)
  env.execute()
}