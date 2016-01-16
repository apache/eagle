package org.apache.eagle.stream.dsl.dataflow.parser

import com.typesafe.config.{Config, ConfigFactory}

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

case class Pipeline(config:Config,dataflow:DataFlow)

/**
 * Pipeline configuration parser
 *
 * For example:
 *
 * {{{
 * <code>
 * {
 *    config {
 *      execution.environment.config = someValue
 *    }
 *    schema {
 *      metricStreamSchema {
 *        metric: string
 *        value: double
 *        timestamp: long
 *      }
 *    }
 *    dataflow {
 *      kafkaSource.source1 {
 *        schema = "metricStreamSchema"
 *      }
 *      kafkaSource.source2 {
 *        schema = {
 *          metric: string
 *          value: double
 *          timestamp: long
 *        }
 *      }
 *    }
 * }
 * </code>
 * }}}
 */
trait PipelineParser{
  val CONFIG_FIELD = "config"
  val SCHEMA_FIELD = "schema"
  val DATAFLOW_FIELD = "dataflow"

  def parse(config:Config):Pipeline = {
    if(config.isEmpty) throw new IllegalArgumentException("Pipeline configuration is empty")
    var pConfig:Config = ConfigFactory.empty()
    var pSchemaSet:SchemaSet = SchemaSet.empty()
    var pDataflow:DataFlow = null
    if(config.hasPath(CONFIG_FIELD)) pConfig = config.getConfig(CONFIG_FIELD)
    if(config.hasPath(SCHEMA_FIELD)) pSchemaSet = SchemaSet.parse(config.getConfig(SCHEMA_FIELD))
    if(config.hasPath(DATAFLOW_FIELD)) pDataflow = DataFlow.parse(config.getConfig(DATAFLOW_FIELD),pSchemaSet)
    new Pipeline(pConfig,pDataflow)
  }
  def parseString(config:String):Pipeline = parse(ConfigFactory.parseString(config))
  def parseResource(resource:String):Pipeline = {
    parse(ConfigFactory.parseResourcesAnySyntax(getClass.getClassLoader,resource))
  }
}