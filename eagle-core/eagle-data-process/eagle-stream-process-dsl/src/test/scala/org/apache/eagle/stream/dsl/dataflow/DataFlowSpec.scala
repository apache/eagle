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
package org.apache.eagle.stream.dsl.dataflow

import com.typesafe.config.ConfigFactory
import org.scalatest.{FlatSpec, Matchers}

class DataFlowSpec extends FlatSpec with Matchers {
  val dataFlowConfig =
    """
       |{
       |	kafkaSource.metric_event_1 {
       |    schema {
       |      metric: string
       |      timestamp: long
       |      value: double
       |    }
       |		parallism = 1000
       |		topic = "metric_event_1"
       |		zkConnection = "localhost:2181"
       |		zkConnectionTimeoutMS = 15000
       |		consumerGroupId = "Consumer"
       |		fetchSize = 1048586
       |		transactionZKServers = "localhost"
       |		transactionZKPort = 2181
       |		transactionZKRoot = "/consumers"
       |		transactionStateUpdateMS = 2000
       |		deserializerClass = "org.apache.eagle.datastream.storm.JsonMessageDeserializer"
       |	}
       |
       |	kafkaSource.metric_event_2 {
       |		schema = {
       |      metric: string
       |      timestamp: long
       |      value: double
       |    }
       |		parallism = 1000
       |		topic = "metric_event_2"
       |		zkConnection = "localhost:2181"
       |		zkConnectionTimeoutMS = 15000
       |		consumerGroupId = "Consumer"
       |		fetchSize = 1048586
       |		transactionZKServers = "localhost"
       |		transactionZKPort = 2181
       |		transactionZKRoot = "/consumers"
       |		transactionStateUpdateMS = 2000
       |		deserializerClass = "org.apache.eagle.datastream.storm.JsonMessageDeserializer"
       |	}
       |
       |	kafkaSink.metricStore {}
       |
       |	alert.alert {
       |		executor = "alertExecutor"
       |	}
       |
       |	aggregator.aggreator {
       |		executor = "aggreationExecutor"
       |	}
       |
       |	metric_event_1|metric_event_2 -> alert {}
       |	metric_event_1|metric_event_2 -> metricStore {}
       |}
     """.stripMargin

  DataFlow.getClass.toString should "parse dataflow end-to-end correctly" in {
    val config = ConfigFactory.parseString(dataFlowConfig)
    config should not be null
    val dataflow = DataFlow.parse(config)
    dataflow should not be null
    dataflow.getConnectors.size should be(4)
    dataflow.getProcessors.size should be(5)
  }

  Identifier.getClass.toString should "parse as definition" in {
    val defId = Identifier.parse("kafka").asInstanceOf[DefinitionIdentifier]
    defId.moduleType should be("kafka")
  }

  Identifier.getClass.toString should "parse node1 -> node2 as connection" in {
    val id = Identifier.parse("node1 -> node2").asInstanceOf[ConnectionIdentifier]
    id.fromIds.size should be(1)
  }

  Identifier.getClass.toString should "parse node1|node2 -> node3" in {
    val id = Identifier.parse("node1|node2 -> node3").asInstanceOf[ConnectionIdentifier]
    id.fromIds.size should be(2)
  }

  Identifier.getClass.toString should "parse node1|node2|node3 -> node4 as connection" in {
    val id = Identifier.parse("node1|node2|node3 -> node4").asInstanceOf[ConnectionIdentifier]
    id.fromIds.size should be(3)
  }

  Identifier.getClass.toString should "parse node1 | node2 | node3 -> node4 as connection" in {
    val id = Identifier.parse("node1 | node2 | node3 -> node4").asInstanceOf[ConnectionIdentifier]
    id.fromIds.size should be(3)
  }
}