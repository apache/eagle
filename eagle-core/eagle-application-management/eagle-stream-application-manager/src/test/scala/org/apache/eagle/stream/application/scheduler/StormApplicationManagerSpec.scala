package org.apache.eagle.stream.application.scheduler

import com.typesafe.config.ConfigFactory
import org.apache.eagle.common.config.EagleConfigConstants
import org.apache.eagle.stream.application.ExecutionPlatform
import org.apache.eagle.stream.application.impl.StormExecutionPlatform

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */


object StormApplicationManagerSpec extends App {
  val manager: ExecutionPlatform = new StormExecutionPlatform
  val baseConfig = ConfigFactory.load()
  val topoConfigStr: String = "webConfig{\"hbase.zookeeper.property.clientPort\":\"2181\", \"hbase.zookeeper.quorum\":\"localhost\"}\nappConfig{\n  \"envContextConfig\" : {\n    \"env\" : \"storm\",\n    \"mode\" : \"cluster\",\n    \"topologyName\" : \"sandbox-hbaseSecurityLog-topology\",\n    \"stormConfigFile\" : \"security-auditlog-storm.yaml\",\n    \"parallelismConfig\" : {\n      \"kafkaMsgConsumer\" : 1,\n      \"hbaseSecurityLogAlertExecutor*\" : 1\n    }\n  },\n  \"dataSourceConfig\": {\n    \"topic\" : \"sandbox_hbase_security_log\",\n    \"zkConnection\" : \"127.0.0.1:2181\",\n    \"zkConnectionTimeoutMS\" : 15000,\n    \"brokerZkPath\" : \"/brokers\",\n    \"fetchSize\" : 1048586,\n    \"deserializerClass\" : \"org.apache.eagle.security.hbase.parse.HbaseAuditLogKafkaDeserializer\",\n    \"transactionZKServers\" : \"127.0.0.1\",\n    \"transactionZKPort\" : 2181,\n    \"transactionZKRoot\" : \"/consumers\",\n    \"consumerGroupId\" : \"eagle.hbasesecurity.consumer\",\n    \"transactionStateUpdateMS\" : 2000\n  },\n  \"alertExecutorConfigs\" : {\n     \"hbaseSecurityLogAlertExecutor\" : {\n       \"parallelism\" : 1,\n       \"partitioner\" : \"org.apache.eagle.policy.DefaultPolicyPartitioner\"\n       \"needValidation\" : \"true\"\n     }\n  },\n  \"eagleProps\" : {\n    \"site\" : \"sandbox\",\n    \"application\": \"hbaseSecurityLog\",\n    \"dataJoinPollIntervalSec\" : 30,\n    \"mailHost\" : \"mailHost.com\",\n    \"mailSmtpPort\":\"25\",\n    \"mailDebug\" : \"true\",\n    \"eagleService\": {\n      \"host\": \"localhost\",\n      \"port\": 9099\n      \"username\": \"admin\",\n      \"password\": \"secret\"\n    }\n  },\n  \"dynamicConfigSource\" : {\n    \"enabled\" : true,\n    \"initDelayMillis\" : 0,\n    \"delayMillis\" : 30000\n  }\n}"

  val topoConfig = ConfigFactory.parseString(topoConfigStr)
  val conf = topoConfig.getConfig(EagleConfigConstants.APP_CONFIG).withFallback(baseConfig)

  //val (ret, nextState) = manager.execute("START", topologyDescModel, null, conf)
  //println(s"Result: ret=$ret, nextState=$nextState")
}



