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
package org.apache.eagle.datastream.storm

import java.io.{File, FileInputStream}

import _root_.storm.trident.spout.RichSpoutBatchExecutor
import backtype.storm.generated.StormTopology
import backtype.storm.utils.Utils
import backtype.storm.{Config, LocalCluster, StormSubmitter}
import org.apache.eagle.common.config.EagleConfigConstants
import org.apache.eagle.datastream.core.AbstractTopologyExecutor
import org.apache.thrift7.transport.TTransportException
import org.slf4j.LoggerFactory
import org.yaml.snakeyaml.Yaml

case class StormTopologyExecutorImpl(topology: StormTopology, config: com.typesafe.config.Config) extends AbstractTopologyExecutor {
  val LOG = LoggerFactory.getLogger(classOf[StormTopologyExecutorImpl])
  @throws(classOf[Exception])
  def execute {
    val localMode: Boolean = config.getString("envContextConfig.mode").equalsIgnoreCase(EagleConfigConstants.LOCAL_MODE)
    val conf: Config = new Config
    conf.put(RichSpoutBatchExecutor.MAX_BATCH_SIZE_CONF, Int.box(64 * 1024))
    conf.put(Config.TOPOLOGY_RECEIVER_BUFFER_SIZE, Int.box(8))
    conf.put(Config.TOPOLOGY_TRANSFER_BUFFER_SIZE, Int.box(32))
    conf.put(Config.TOPOLOGY_EXECUTOR_RECEIVE_BUFFER_SIZE, Int.box(16384))
    conf.put(Config.TOPOLOGY_EXECUTOR_SEND_BUFFER_SIZE, Int.box(16384))
    conf.put(Config.NIMBUS_THRIFT_MAX_BUFFER_SIZE, Int.box(20480000))

    if(config.hasPath("envContextConfig.stormConfigFile")) {
      val file = new File(config.getString("envContextConfig.stormConfigFile"))
      if(file.exists()) {
        val inputFileStream = new FileInputStream(file)
        val yaml = new Yaml()
        try {
          val stormConf = yaml.load(inputFileStream).asInstanceOf[java.util.LinkedHashMap[String, Object]]
          if(stormConf != null) conf.putAll(stormConf)
        } catch {
          case t: Throwable => {
            LOG.error(s"Got example $t",t)
            throw t
          }
        } finally {
          if(inputFileStream != null) inputFileStream.close()
        }
      }
    }

    val topologyName = config.getString("envContextConfig.topologyName")
    if (!localMode) {
      if(config.hasPath("envContextConfig.nimbusHost")) {
        LOG.info(s"Setting ${backtype.storm.Config.NIMBUS_HOST} as ${config.getString("envContextConfig.nimbusHost")}")
        conf.put(backtype.storm.Config.NIMBUS_HOST, config.getString("envContextConfig.nimbusHost"))
      }

      if(config.hasPath("envContextConfig.nimbusThriftPort")) {
        LOG.info(s"Setting ${backtype.storm.Config.NIMBUS_THRIFT_PORT} as ${config.getString("envContextConfig.nimbusThriftPort")}")
        conf.put(backtype.storm.Config.NIMBUS_THRIFT_PORT, config.getNumber("envContextConfig.nimbusThriftPort"))
      }

      if(config.hasPath("envContextConfig.jarFile")){
        LOG.info(s"Setting storm.jar as ${config.getString("envContextConfig.jarFile")}")
        System.setProperty("storm.jar",config.getString("envContextConfig.jarFile"))
      }

      LOG.info("Submitting as cluster mode")
      try {
        StormSubmitter.submitTopologyWithProgressBar(topologyName, conf, topology)
      } catch {
        case e:TTransportException =>
          LOG.error(s"Got thrift exception, type: ${e.getType}")
          throw e
      }
      finally {
        System.clearProperty("storm.jar")
      }
    } else {
      LOG.info("Submitting as local mode")
      val cluster: LocalCluster = new LocalCluster
      cluster.submitTopology(topologyName, conf, topology)
      while(true) {
        try {
          Utils.sleep(Integer.MAX_VALUE)
        } catch {
          case _: Throwable =>
            cluster.killTopology(topologyName)
            cluster.shutdown
            //throw new Exception("Catch a runtime exception during sleeping")
        }
      }
    }
  }
}