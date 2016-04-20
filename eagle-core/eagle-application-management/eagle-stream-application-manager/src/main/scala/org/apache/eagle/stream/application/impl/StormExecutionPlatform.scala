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
import org.apache.eagle.service.application.AppManagerConstants
import org.apache.eagle.service.application.entity.{TopologyDescriptionEntity, TopologyExecutionEntity, TopologyExecutionStatus}
import org.apache.eagle.stream.application.{ApplicationManager, ApplicationManagerUtils, ExecutionPlatform, TopologyFactory}
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions

object StormExecutionPlatform {
  val ACTIVE: String = "ACTIVE"
  val INACTIVE: String = "INACTIVE"
  val KILLED: String = "KILLED"
  val REBALANCING: String = "REBALANCING"
}

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
          val topologyType = topology.getType.toUpperCase()
          topologyType match {
            case TopologyDescriptionEntity.TYPE.CLASS =>
              TopologyFactory.submit(topology.getExeClass, config)
            case TopologyDescriptionEntity.TYPE.DYNAMIC =>
              StormDynamicTopology.submit(topology.getExeClass, config)
            case m@_ =>
              throw new InvalidTopologyException("Unsupported topology type: " + topology.getType)
          }
        } catch {
          case ex: Throwable =>
            LOG.error(s"Starting topology $topologyName in local failed due to ${ex.getMessage}")
        }
      }
    })
    topologyExecution.setFullName(topologyName)
    topologyExecution.setStatus(ApplicationManager.getWorkerStatus(worker.getState))
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
    val extConfigStr = "envContextConfig.topologyName=%s".format(fullName)
    val extConfig = ConfigFactory.parseString(extConfigStr)
    val newConfig = extConfig.withFallback(config)

    val mode = if(config.hasPath(AppManagerConstants.RUNNING_MODE)) config.getString(AppManagerConstants.RUNNING_MODE) else EagleConfigConstants.LOCAL_MODE
    topologyExecution.setMode(mode)
    if (topologyExecution.getMode.equalsIgnoreCase(EagleConfigConstants.LOCAL_MODE)) {
      startLocal(fullName, topology, topologyExecution, newConfig)
      return
    }

    val topologyType = topology.getType.toUpperCase()
    topologyType match {
      case TopologyDescriptionEntity.TYPE.CLASS =>
        TopologyFactory.submit(topology.getExeClass, newConfig)
      case TopologyDescriptionEntity.TYPE.DYNAMIC =>
        StormDynamicTopology.submit(topology.getExeClass, newConfig)
      case m@_ =>
        throw new InvalidTopologyException("Unsupported topology type: " + topology.getType)
    }
    topologyExecution.setFullName(fullName)
    //topologyExecution.setStatus(TopologyExecutionStatus.STARTED)
  }

  override def stop(topologyExecution: TopologyExecutionEntity, config: Config): Unit = {
    val name: String = ApplicationManagerUtils.generateTopologyFullName(topologyExecution)

    if(topologyExecution.getMode.equalsIgnoreCase(EagleConfigConstants.LOCAL_MODE)) {
      stopLocal(name, topologyExecution)
    } else {
      getNimbusClient(config).getClient.killTopology(name)
      topologyExecution.setStatus(TopologyExecutionStatus.STOPPING)
      //topologyExecution.setDescription("")
    }
  }

  def stopLocal(name: String, topologyExecution: TopologyExecutionEntity): Unit = {
      val taskWorker = ApplicationManager.stop(name)
      topologyExecution.setStatus(ApplicationManager.getWorkerStatus(taskWorker.getState))
      topologyExecution.setDescription(s"topology status is ${taskWorker.getState}")
      /*try{
        ApplicationManager.remove(name)
      } catch {
        case ex: IllegalArgumentException =>
          LOG.warn(s"ApplicationManager.remove($name) failed as it has been removed")
      }*/
  }


  def getTopology(topologyName: String, config: Config) = {
    val topologySummery = getNimbusClient(config).getClient.getClusterInfo.get_topologies
    JavaConversions.collectionAsScalaIterable(topologySummery).find { t => t.get_name.equals(topologyName) }
    match {
      case Some(t) => Some(t)
      case None    => None
    }
  }

  override def status(topologyExecution: TopologyExecutionEntity, config: Config): Unit = {
    val name: String = ApplicationManagerUtils.generateTopologyFullName(topologyExecution)

    if(topologyExecution.getMode.equalsIgnoreCase(EagleConfigConstants.LOCAL_MODE)) {
      statusLocal(name, topologyExecution)
    } else {
      val topology = getTopology(name, config)
      topology match {
        case Some(topology) =>
          topologyExecution.setStatus(ApplicationManager.getTopologyStatus(topology.get_status()))
          topologyExecution.setUrl(ApplicationManagerUtils.buildStormTopologyURL(config, topology.get_id()))
          topologyExecution.setDescription(topology.toString)
        case None =>
          topologyExecution.setStatus(TopologyExecutionStatus.STOPPED)
          topologyExecution.setUrl("")
          topologyExecution.setDescription(s"Fail to find topology: $name")
      }
    }
  }

  def statusLocal(name: String, topologyExecution: TopologyExecutionEntity): Unit = {
    try {
      val currentStatus = topologyExecution.getStatus()
      val newStatus = ApplicationManager.getWorkerStatus(ApplicationManager.get(name).getState())
      if (!currentStatus.equals(newStatus)) {
        LOG.info("Status of topology: %s changed from %s to %s".format(topologyExecution.getFullName, currentStatus, newStatus))
        topologyExecution.setStatus(newStatus)
        topologyExecution.setDescription(String.format("Status of topology: %s changed from %s to %s", name, currentStatus, newStatus))
      } else if(currentStatus.equalsIgnoreCase(TopologyExecutionStatus.STOPPED)) {
        ApplicationManager.remove(name)
      }
    }catch {
      case ex: Throwable =>
        topologyExecution.setDescription("")
        topologyExecution.setStatus(TopologyExecutionStatus.STOPPED)
    }
  }

  override def status(topologyExecutions: java.util.List[TopologyExecutionEntity], config: Config): Unit = {
    JavaConversions.collectionAsScalaIterable(topologyExecutions) foreach {
      topologyExecution => status(topologyExecution, config)
    }
  }
}
