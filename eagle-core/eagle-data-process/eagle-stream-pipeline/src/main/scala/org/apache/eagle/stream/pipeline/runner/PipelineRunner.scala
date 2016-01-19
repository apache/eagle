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
import org.slf4j.LoggerFactory

import scala.reflect.runtime.{universe => ru}

trait PipelineRunner extends PipelineParser with PipelineCompiler{
  import PipelineCLIOptionParser._
  private val LOG = LoggerFactory.getLogger("PipelineCLIOptionParser")
  def submit[T <: ExecutionEnvironment](resource:String)(implicit typeTag:ru.TypeTag[T]) =
    compile(parseResource(resource)).submit[T]
  def submit(resource:String,clazz:Class[ExecutionEnvironment]) =
    compile(parseResource(resource)).submit(clazz)
  def submit(pipelineConfig:Config,clazz:Class[ExecutionEnvironment]) =
    compile(parse(pipelineConfig)).submit(clazz)
  def submit[T <: ExecutionEnvironment](pipelineConfig:Config)(implicit typeTag: ru.TypeTag[T]) =
    compile(parse(pipelineConfig)).submit[T]

  def apply(args:Array[String]):PipelineRunner = {
    new ConfigOptionParser().load(args)
    this
  }

  def main(args: Array[String]): Unit = {
    val config = PipelineCLIOptionParser.load(args)
    submit[storm](config.getString(PIPELINE_CONFIG_KEY))
  }
}

private[runner] object PipelineCLIOptionParser extends ConfigOptionParser{
  val LOG = LoggerFactory.getLogger("PipelineCLIOptionParser")
  val PIPELINE_OPT_KEY="pipeline"

  val PIPELINE_CONFIG_KEY="pipeline.config"

  val CONFIG_OPT_KEY="config"
  val CONFIG_RESOURCE_KEY="config.resource"
  val CONFIG_FILE_KEY="config.file"
  val USAGE =
    """
      |Usage: java org.apache.eagle.stream.pipeline.Pipeline [options]
      |
      |Options:
      |   --pipeline   pipeline configuration
      |   --config     common configuration
      |   --env        storm (support spark, etc later)
      |   --mode       local/remote/cluster
    """.stripMargin
  
  override protected def options(): Options = {
    val options = super.options()
    options.addOption(PIPELINE_OPT_KEY, true, "Pipeline configuration file")
    options.addOption(CONFIG_OPT_KEY, true, "Config properties file")
    options
  }

  override protected def parseCommand(cmd: CommandLine): util.Map[String, String] = {
    val map = super.parseCommand(cmd)
    if (cmd.hasOption(PIPELINE_OPT_KEY)) {
      val pipelineConf = cmd.getOptionValue(PIPELINE_OPT_KEY)
      if(pipelineConf == null){
        throw new IllegalArgumentException(s"$PIPELINE_OPT_KEY should not be null")
      } else {
        LOG.info(s"Set $PIPELINE_CONFIG_KEY as $pipelineConf")
        map.put(PIPELINE_CONFIG_KEY, pipelineConf)
      }
    }else {
      sys.error(
        s"""
           |Error: --$PIPELINE_OPT_KEY is required
           |$USAGE
         """.stripMargin)
    }

    if(cmd.hasOption(CONFIG_OPT_KEY)){
      val commonConf = cmd.getOptionValue(CONFIG_OPT_KEY)
      if(commonConf.contains("/")){
        LOG.info(s"Set $CONFIG_FILE_KEY as $commonConf")
        map.put(CONFIG_FILE_KEY, commonConf)
      }else {
        LOG.info(s"Set $CONFIG_RESOURCE_KEY $commonConf")
        map.put(CONFIG_RESOURCE_KEY, commonConf)
      }
    }
    map
  }
}