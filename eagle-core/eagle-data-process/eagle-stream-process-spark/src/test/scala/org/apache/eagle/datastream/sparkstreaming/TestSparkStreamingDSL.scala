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
package org.apache.eagle.datastream.sparkstreaming

import org.apache.eagle.datastream.ExecutionEnvironments
import org.apache.eagle.datastream.storm.JsonMessageDeserializer


case class Entity(name: String, value: Double, var inc: Int = 0) {
  override def toString = {
    "name=" + name + "," + "value=" + value + "," + "inc=" + inc
  }
}

object TestSparkStreamingDSL extends App {
  val env = ExecutionEnvironments.get[SparkStreamingExecutionEnvironment](args)
  val tuple = Seq(
    Entity("a", 1),
    Entity("a", 2),
    Entity("a", 3),
    Entity("b", 2),
    Entity("c", 3),
    Entity("d", 3)
  )

  val tmp = Seq("STORM KAFKA", "ZOOKEEPER LOG", "SPARK STREAMING", "SPARK HADOOP")

  env.from(tuple, recycle = true)
    .groupByKey(_.name)
    .map(o => {
      o.inc += 2; o
    })
    .filter(_.name != "b")
    .filter(_.name != "c")
    .groupByKey(o => (o.name, o.value))
    .map(o => (o.name, o))
    .map(o => (o._1, o._2.value, o._2.inc))
    .foreach(println)

  env.from(tmp, recycle = true)
    .flatMap(_.split(" "))
    .map(o => (o, 1))
    .reduceByKey((x: Int, y: Int) => x + y)
    .filter(o => !o._1.contains("zqin"))
    .foreach(println)

  env.execute()
}

object TestSparkStreamingWithAlertDSL extends App {
  val env = ExecutionEnvironments.get[SparkStreamingExecutionEnvironment](args)
  val streamName = "cassandraQueryLogStream"
  val streamExecutorId = "cassandraQueryLogExecutor"
  env.config.set("dataSourceConfig.deserializerClass", classOf[JsonMessageDeserializer].getCanonicalName)
  env.fromKafka().parallelism(1).nameAs(streamName).!(Seq(streamName), streamExecutorId)
  env.execute()
}