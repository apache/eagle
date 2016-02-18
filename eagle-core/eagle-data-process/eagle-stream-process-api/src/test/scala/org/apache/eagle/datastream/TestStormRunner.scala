/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.eagle.datastream

import com.typesafe.config.{Config, ConfigFactory}

/**
 * explicit union
 * a.union(b,c).alert() means (a,b,c)'s output is united into alert()
 * before running this testing, we should define in eagle service one policy and one stream schema
 * 1. stream schema
 * curl -X POST -H 'Content-Type:application/json' "http://${EAGLE_SERVICE_HOST}:${EAGLE_SERVICE_PORT}/eagle-service/rest/entities?serviceName=AlertStreamSchemaService" -d '[{"prefix":"alertStreamSchema","tags":{"dataSource":"ds1","streamName":"s1","attrName":"word"},"attrDescription":"word","attrType":"string","category":"","attrValueResolver":""}]'
 * 2. policy
 * curl -X POST -H 'Content-Type:application/json' "http://${EAGLE_SERVICE_HOST}:${EAGLE_SERVICE_PORT}/eagle-service/rest/entities?serviceName=AlertDefinitionService" -d '[{"tags":{"site":"sandbox","dataSource":"ds1","alertExecutorId":"alert1","policyId":"testAlert","policyType":"siddhiCEPEngine"},"desc":"test alert","policyDef":"{\"type\":\"siddhiCEPEngine\",\"expression\":\"from s1 [(str:regexp(word,'\'.*test.*\)==true)] select * insert into outputStream ;\"}","dedupeDef":"","notificationDef":"","remediationDef":"","enabled":"true"}]'
 */
object UnionForAlert extends App{
  val config : Config = ConfigFactory.load;
  val env = ExecutionEnvironments.getStorm(config)
  val tail1 = env.fromSpout(TestSpout()).flatMap(WordPrependForAlertExecutor("test")).map2(a => ("key1",a))
  val tail2 = env.fromSpout(TestSpout()).flatMap(WordAppendForAlertExecutor("test")).map2(a => ("key2",a))
  tail1.streamUnion(List(tail2)).alert(Seq("s1","s2"), "alert1", consume = false)
  env.execute()
}

/**
 * test alert after flatMap
 */
object TestAlertAfterFlatMap extends App{
  val config : Config = ConfigFactory.load;
  val env = ExecutionEnvironments.getStorm(config)
  val tail1 = env.fromSpout(TestSpout())
                  .flatMap(WordPrependForAlertExecutor("test"))
                  .alert(Seq("s1"), "alert1", consume = false)
  //env.execute
}

/**
 * test alert after Map
 */
object TestAlertAfterMap extends App{
  val config : Config = ConfigFactory.load;
  val env = ExecutionEnvironments.getStorm(config)
  val tail1 = env.fromSpout(TestSpout())
    .flatMap(WordPrependForAlertExecutor2("test"))
    .map2(a => ("key", a))
    .alert(Seq("s1"), "alert1", false)
  //env.execute
}

object StormRunnerWithoutSplitOrJoin extends Application{
  val config : Config = ConfigFactory.load;
  val env = ExecutionEnvironments.getStorm(config)
  env.fromSpout(TestSpout()).flatMap(EchoExecutor()).flatMap(WordPrependExecutor("test"))
    .flatMap(PatternAlertExecutor("test.*"))
//  env.execute()
}

object StormRunnerWithSplit extends Application{
  val config : Config = ConfigFactory.load;
  val env = ExecutionEnvironments.getStorm(config)
  val toBeSplit = env.fromSpout(TestSpout()).flatMap(EchoExecutor())
  toBeSplit.flatMap(WordPrependExecutor("test")).flatMap(PatternAlertExecutor("test.*"))
  toBeSplit.flatMap(WordAppendExecutor("test"))
//  env.execute()
}

object StormRunnerWithUnion extends Application{
  val config : Config = ConfigFactory.load;
  val env = ExecutionEnvironments.getStorm(config)
  val tail1 = env.fromSpout(TestSpout()).flatMap(WordPrependExecutor("test"))
  val tail2 = env.fromSpout(TestSpout()).flatMap(WordAppendExecutor("test"))
  tail1.streamUnion(List(tail2)).flatMap(PatternAlertExecutor(".*test.*"))
  env.execute()
}

object StormRunnerWithFilter extends Application{
  val config : Config = ConfigFactory.load;
  val env = ExecutionEnvironments.getStorm(config)
  env.fromSpout(TestSpout()).flatMap(EchoExecutor()).flatMap(WordPrependExecutor("test")).
    filter(_=>false).
    flatMap(PatternAlertExecutor("test.*"))
  //env.execute
}

object StormRunnerWithJavaExecutor extends Application{
  val config : Config = ConfigFactory.load;
  val env = ExecutionEnvironments.getStorm(config)
  env.fromSpout(TestSpout()).flatMap(new JavaEchoExecutor()).flatMap(WordPrependExecutor("test")).
    filter(_=>false).
    flatMap(PatternAlertExecutor("test.*"))
  //env.execute
}

object StormRunnerWithKeyValueSpout extends Application{
  val config : Config = ConfigFactory.load;
  val env = ExecutionEnvironments.getStorm(config)
  env.fromSpout(TestKeyValueSpout()).groupBy(1).flatMap(new GroupedEchoExecutor()).parallelism(2)
  //env.execute
}

object StormRunnerWithKeyValueSpoutRenameOutputFields extends Application{
  val config : Config = ConfigFactory.load;
  val env = ExecutionEnvironments.getStorm(config)
  env.fromSpout(TestKeyValueSpout()).withOutputFields(2).groupBy(0).flatMap(new GroupedEchoExecutor()).parallelism(2)
  //env.execute
}