/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

package org.apache.eagle.stream.application.impl

import java.net.URLDecoder
import java.nio.file.{Files, Paths}

import backtype.storm.generated.InvalidTopologyException
import backtype.storm.utils.{NimbusClient, Utils}
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.eagle.common.config.EagleConfigConstants
import org.apache.eagle.service.application.entity.{TopologyExecutionStatus, TopologyDescriptionEntity, TopologyExecutionEntity}
import org.apache.eagle.stream.application.{ApplicationManager, ApplicationManagerUtils, ExecutionPlatform, TopologyFactory}
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions


class StormExecutionPlatform extends ExecutionPlatform {
  val LOG = LoggerFactory.getLogger(classOf[StormExecutionPlatform])

  private def getNimbusClient(appConfig: com.typesafe.config.Config): NimbusClient = {
    val conf = Utils.readStormConfig().asInstanceOf[java.util.HashMap[String, Object]]
    conf.putAll(Utils.readCommandLineOpts().asInstanceOf[java.util.HashMap[String, Object]])

    if(appConfig.hasPath("envContextConfig.nimbusHost")) {
      LOG.info(s"Setting ${backtype.storm.Config.NIMBUS_HOST} as ${appConfig.getString("envContextConfig.nimbusHost")}")
      conf.put(backtype.storm.Config.NIMBUS_HOST, appConfig.getString("envContextConfig.nimbusHost"))
    }

    if(appConfig.hasPath("envContextConfig.nimbusThriftPort")) {
      LOG.info(s"Setting ${backtype.storm.Config.NIMBUS_THRIFT_PORT} as ${appConfig.getString("envContextConfig.nimbusThriftPort")}")
      conf.put(backtype.storm.Config.NIMBUS_THRIFT_PORT, appConfig.getNumber("envContextConfig.nimbusThriftPort"))
    }
    NimbusClient.getConfiguredClient(conf)
  }

  def startLocal(topologyName: String, topology: TopologyDescriptionEntity, topologyExecution: TopologyExecutionEntity, config: Config): Unit = {
    val worker: Thread = ApplicationManager.submit(topologyName, new Runnable {
      override def run(): Unit = {
        try {
          topology.getType match {
            case TopologyDescriptionEntity.TYPE.CLASS =>
              TopologyFactory.submit(topology.getExeClass, config)
            case TopologyDescriptionEntity.TYPE.DYNAMIC =>
              StormDynamicTopology.submit(topology.getExeClass, config)
            case m@_ =>
              throw new InvalidTopologyException("Unsupported topology type: " + topology.getType)
          }
        }
        topologyExecution.setFullName(topologyName)
        topologyExecution.setStatus(TopologyExecutionStatus.STARTED)
      }
    })
    topologyExecution.setDescription("Running inside " + worker.toString + " in local mode")
  }

  override def start(topology: TopologyDescriptionEntity, topologyExecution: TopologyExecutionEntity, config: Config): Unit = {
    val stormJarPath: String = URLDecoder.decode(classOf[ExecutionPlatform].getProtectionDomain.getCodeSource.getLocation.getPath, "UTF-8")
    if (stormJarPath == null || !Files.exists(Paths.get(stormJarPath)) || !stormJarPath.endsWith(".jar")) {
      val errMsg = s"storm jar file $stormJarPath does not exists, or is a invalid jar file"
      LOG.error(errMsg)
      throw new Exception(errMsg)
    }
    LOG.info(s"Detected a storm.jar location at: $stormJarPath")
    System.setProperty("storm.jar", stormJarPath)

    val fullName = ApplicationManagerUtils.generateTopologyFullName(topologyExecution)
    val extConfigStr = "envContextConfig.topologyName=%s, envContextConfig.mode=%s".format(fullName, topologyExecution.getMode)
    val extConfig = ConfigFactory.parseString(extConfigStr)
    val conf = extConfig.withFallback(config)

    if (topologyExecution.getEnvironment.equalsIgnoreCase(EagleConfigConstants.LOCAL_MODE)) {
      startLocal(fullName, topology, topologyExecution, config)
      return
    }

    try {
      topology.getType match {
        case TopologyDescriptionEntity.TYPE.CLASS =>
          TopologyFactory.submit(topology.getExeClass, conf)
        case TopologyDescriptionEntity.TYPE.DYNAMIC =>
          StormDynamicTopology.submit(topology.getExeClass, conf)
        case m@_ =>
          throw new InvalidTopologyException("Unsupported topology type: " + topology.getType)
      }
    }
    topologyExecution.setFullName(fullName)
    topologyExecution.setStatus(TopologyExecutionStatus.STARTED)
  }

  override def stop(topologyExecution: TopologyExecutionEntity, config: Config): Unit = {
    val name: String = ApplicationManagerUtils.generateTopologyFullName(topologyExecution)

    if(topologyExecution.getEnvironment.equalsIgnoreCase(EagleConfigConstants.LOCAL_MODE)) {
      stopLocal(name, topologyExecution)
    }

    getNimbusClient(config).getClient.killTopology(name)
    topologyExecution.setStatus(TopologyExecutionStatus.STOPPED)
  }

  def stopLocal(name: String, topologyExecution: TopologyExecutionEntity): Unit = {
    ApplicationManager.stop(name)
    topologyExecution.setStatus(TopologyExecutionStatus.STOPPED)
    ApplicationManager.remove(name)
  }

  override def status(topologyExecution: TopologyExecutionEntity, config: Config): Unit = {
    val name: String = ApplicationManagerUtils.generateTopologyFullName(topologyExecution)

    if(topologyExecution.getEnvironment.equalsIgnoreCase(EagleConfigConstants.LOCAL_MODE)) {
      statusLocal(name, topologyExecution)
    }
  }

  def statusLocal(name: String, topologyExecution: TopologyExecutionEntity): Unit = {

  }

  override def status(topologyExecutions: java.util.List[TopologyExecutionEntity], config: Config): Unit = {
    JavaConversions.collectionAsScalaIterable(topologyExecutions) foreach {
      topologyExecution => status(topologyExecution, config)
    }
  }
}
