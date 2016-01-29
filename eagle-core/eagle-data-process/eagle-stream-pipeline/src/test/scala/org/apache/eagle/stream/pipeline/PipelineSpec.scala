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

import org.apache.eagle.datastream.ExecutionEnvironments.storm
import org.scalatest.{FlatSpec, Matchers}

class PipelineSpec extends FlatSpec with Matchers{
  "Pipeline" should "parse successfully from pipeline_1.conf" in {
    val pipeline = Pipeline.parseResource("pipeline_1.conf")
    pipeline should not be null
  }

  "Pipeline" should "compile successfully from pipeline_2.conf" in {
    val pipeline = Pipeline.parseResource("pipeline_2.conf")
    pipeline should not be null
    val stream = Pipeline.compile(pipeline)
    stream should not be null
    // Throw ClassNotFoundException when submit in unit test
    // stream.submit[storm]
  }
}

/**
 * Storm LocalCluster throws ClassNotFoundException when submit in unit test, so here submit in App
 */
object PipelineSpec_2 extends App{
  val pipeline = Pipeline(args).parseResource("pipeline_2.conf")
  val stream = Pipeline.compile(pipeline)
  stream.submit[storm]
}

object PipelineSpec_3 extends App {
  Pipeline(args).submit[storm]("pipeline_3.conf")
}

object PipelineSpec_4 extends App {
  Pipeline(args).submit[storm]("pipeline_4.conf")
}

object PipelineSpec_5 extends App {
  Pipeline(args).submit[storm]("pipeline_5.conf")
}

object PipelineCLISpec extends App{
  Pipeline.main(args)
}