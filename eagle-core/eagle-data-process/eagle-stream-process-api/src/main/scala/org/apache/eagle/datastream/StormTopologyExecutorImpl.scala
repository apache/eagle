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

import java.io.{FileInputStream, File}

import _root_.storm.trident.spout.RichSpoutBatchExecutor
import backtype.storm.generated.StormTopology
import backtype.storm.utils.Utils
import backtype.storm.{Config, LocalCluster, StormSubmitter}
import org.yaml.snakeyaml.Yaml

case class StormTopologyExecutorImpl(topology: StormTopology, config: com.typesafe.config.Config) extends AbstractTopologyExecutor {
  @throws(classOf[Exception])
  def execute {
    val localMode: Boolean = config.getString("envContextConfig.mode").equalsIgnoreCase("local")
    val conf: Config = new Config
    conf.put(RichSpoutBatchExecutor.MAX_BATCH_SIZE_CONF, Int.box(64 * 1024))
    conf.put(Config.TOPOLOGY_RECEIVER_BUFFER_SIZE, Int.box(8))
    conf.put(Config.TOPOLOGY_TRANSFER_BUFFER_SIZE, Int.box(32))
    conf.put(Config.TOPOLOGY_EXECUTOR_RECEIVE_BUFFER_SIZE, Int.box(16384))
    conf.put(Config.TOPOLOGY_EXECUTOR_SEND_BUFFER_SIZE, Int.box(16384))

    if(config.hasPath("envContextConfig.stormConfigFile")) {
      //val inputFileStream = {
      //  StormTopologyExecutorImpl.getClass.getClassLoader.getResourceAsStream(config.getString("envContextConfig.stormConfigFile"))
      //}
      val file = new File(config.getString("envContextConfig.stormConfigFile"))
      if(file.exists()) {
        val inputFileStream = new FileInputStream(file)
        val yaml = new Yaml()
        try {
          val stormConf = yaml.load(inputFileStream).asInstanceOf[java.util.LinkedHashMap[String, Object]]
          if(stormConf != null) conf.putAll(stormConf)
        } catch {
          case _: Throwable => ()
        } finally {
          if(inputFileStream != null) inputFileStream.close()
        }
      }
    }

    val topologyName = config.getString("envContextConfig.topologyName")
    if (!localMode) {
      StormSubmitter.submitTopologyWithProgressBar(topologyName, conf, topology)
    }
    else {
      val cluster: LocalCluster = new LocalCluster
      cluster.submitTopology(topologyName, conf, topology)
      while(true) {
        try {
          Utils.sleep(Integer.MAX_VALUE)
        }
        catch {
          case _: Throwable => () // Do nothing
        }
      }
    }
  }
}