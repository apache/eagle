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
package org.apache.eagle.stream.pipeline.runner

import java.util

import com.typesafe.config.Config
import org.apache.commons.cli.{CommandLine, Options}
import org.apache.eagle.dataproc.util.ConfigOptionParser
import org.apache.eagle.datastream.ExecutionEnvironments.storm
import org.apache.eagle.datastream.core.ExecutionEnvironment
import org.apache.eagle.stream.pipeline.compiler.PipelineCompiler
import org.apache.eagle.stream.pipeline.parser.PipelineParser

import scala.reflect.runtime.{universe => ru}

trait PipelineRunner extends PipelineParser with PipelineCompiler{
  def submit[T <: ExecutionEnvironment](resource:String)(implicit typeTag:ru.TypeTag[T]) = compile(parseResource(resource)).submit[T]
  def submit(resource:String,clazz:Class[ExecutionEnvironment]) = compile(parseResource(resource)).submit(clazz)
  def submit(pipelineConfig:Config,clazz:Class[ExecutionEnvironment]) = compile(parse(pipelineConfig)).submit(clazz)
  def apply(args:Array[String]):PipelineRunner = {
    new ConfigOptionParser().load(args)
    this
  }

  def main(args: Array[String]): Unit = {
    val config = PipelineCLIOptionParser.load(args)
    // TODO: Load environment, currently hard-code with storm
    submit[storm](config.getString(PipelineCLIOptionParser.PIPELINE_RESOURCE_KEY))
  }
}

private[runner] object PipelineCLIOptionParser extends ConfigOptionParser{
  val PIPELINE_OPT_KEY="pipeline"
  val PIPELINE_RESOURCE_KEY="pipeline.resource"
  val CONFIG_OPT_KEY="config"
  val CONFIG_RESOURCE_KEY="config.resource"

  override protected def options(): Options = {
    val options = super.options()
    options.addOption(PIPELINE_OPT_KEY, true, "Pipeline configuration file")
    options.addOption(CONFIG_OPT_KEY, true, "Config properties file")
    options
  }

  override protected def parseCommand(cmd: CommandLine): util.Map[String, String] = {
    val map = super.parseCommand(cmd)
    if (cmd.hasOption(PIPELINE_OPT_KEY)) {
      map.put(PIPELINE_RESOURCE_KEY,cmd.getOptionValue(PIPELINE_OPT_KEY))
    }else{
      throw new IllegalArgumentException("--"+PIPELINE_OPT_KEY +" is required")
    }
    if(cmd.hasOption(CONFIG_OPT_KEY)){
      map.put(CONFIG_RESOURCE_KEY,cmd.getOptionValue(CONFIG_OPT_KEY))
    }
    map
  }
}