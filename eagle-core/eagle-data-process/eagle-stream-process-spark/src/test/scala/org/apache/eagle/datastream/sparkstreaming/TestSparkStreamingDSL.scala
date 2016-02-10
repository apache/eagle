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


case class Entity(name:String,value:Double,var inc:Int=0){
  override def toString = {
    "name="+name+","+"value="+value+","+"inc="+inc
  }
}

object TestSparkStreamingDSL extends App{
  val env = ExecutionEnvironments.get[SparkStreamingExecutionEnvironment](args)
  val tuples = Seq(
    Entity("a", 1),
    Entity("a", 2),
    Entity("a", 3),
    Entity("b", 2),
    Entity("c", 3),
    Entity("d", 3)
  )

  val tmp  = Array("ebay","zqin","twen","arsenal")

  env.from(tuples,recycle = true)
    .map(o => {o.inc += 2;o})
    .filter(o => !o.name.equals("c"))
    .foreach(println)

  env.from(tmp,recycle = true)
  .map(t => t+" from test")
  .filter(t => !t.contains("al"))
  .foreach(println)

  env.execute()

}