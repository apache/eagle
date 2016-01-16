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
package org.apache.eagle.datastream.storm

import backtype.storm.topology.base.BaseRichSpout
import com.typesafe.config.Config
import org.apache.eagle.dataproc.impl.storm.StormSpoutProvider
import org.apache.eagle.datastream.core.{ExecutionEnvironment, StormSourceProducer, StreamDAG}

/**
 * @since  12/7/15
 */
class StormExecutionEnvironment(private val conf:Config) extends ExecutionEnvironment(conf) {
  override def execute(dag: StreamDAG) : Unit = {
    StormTopologyCompiler(config.get, dag).buildTopology.execute
  }

  def fromSpout[T](source: BaseRichSpout): StormSourceProducer[T] = {
    val ret = StormSourceProducer[T](source)
    ret.initWith(dag,config.get)
    ret
  }

  def fromSpout[T](sourceProvider: StormSpoutProvider):StormSourceProducer[T] = fromSpout(sourceProvider.getSpout(config.get))
}