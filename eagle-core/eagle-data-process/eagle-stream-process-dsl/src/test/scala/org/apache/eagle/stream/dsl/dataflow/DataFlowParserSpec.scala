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

class DataFlowParserSpec extends FlatSpec with Matchers {
  DataFlow.getClass.toString should "parse pipeline.conf correctly" in {
    val config = ConfigFactory.load(classOf[DataFlowParserSpec].getClassLoader,"pipeline")
    config should not be null
    val pipeline = DataFlow.parse(config.getConfig("pipeline"))
    pipeline should not be null
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