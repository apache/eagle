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
package org.apache.eagle.stream.pipeline


import com.typesafe.config.Config
import org.apache.eagle.dataproc.util.ConfigOptionParser
import org.apache.eagle.datastream.core.ExecutionEnvironment
import org.apache.eagle.stream.pipeline.compiler.PipelineCompiler
import org.apache.eagle.stream.pipeline.parser.PipelineParser

import scala.reflect.runtime.{universe => ru}

class PipelineSubmitter extends PipelineParser with PipelineCompiler{
  def submit[T <: ExecutionEnvironment](resource:String)(implicit typeTag:ru.TypeTag[T]) = compile(parseResource(resource)).submit[T]
  def submit(resource:String,clazz:Class[ExecutionEnvironment]) = compile(parseResource(resource)).submit(clazz)
  def submit(pipelineConfig:Config,clazz:Class[ExecutionEnvironment]) = compile(parse(pipelineConfig)).submit(clazz)
  def apply(args:Array[String]):PipelineSubmitter = {
    new ConfigOptionParser().load(args)
    this
  }
}

object Pipeline extends PipelineSubmitter
  with PipelineParser
  with PipelineCompiler