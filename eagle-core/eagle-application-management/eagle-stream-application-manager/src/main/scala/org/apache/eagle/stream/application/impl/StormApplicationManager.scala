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
import com.typesafe.config
import org.apache.eagle.service.application.entity.TopologyDescriptionEntity
import org.apache.eagle.stream.application.model.TopologyDescriptionModel
import org.apache.eagle.stream.application.{ApplicationManager, TopologyFactory}
import org.slf4j.LoggerFactory


class StormApplicationManager extends ApplicationManager {
  val LOG = LoggerFactory.getLogger(classOf[StormApplicationManager])

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

  override def start(topologyDesc: TopologyDescriptionModel, conf: config.Config): Boolean = {
    var ret = true
    try {
      val stormJarPath: String = URLDecoder.decode(classOf[ApplicationManager].getProtectionDomain.getCodeSource.getLocation.getPath, "UTF-8")
      if (stormJarPath == null || !Files.exists(Paths.get(stormJarPath)) || !stormJarPath.endsWith(".jar")) {
        val errMsg = s"storm jar file $stormJarPath does not exists, or is a invalid jar file"
        LOG.error(errMsg)
        throw new Exception(errMsg)
      }
      LOG.info(s"Detected a storm.jar location at: $stormJarPath")
      System.setProperty("storm.jar", stormJarPath)

      topologyDesc.topoType match {
        case TopologyDescriptionEntity.TYPE.CLASS =>
          TopologyFactory.submit(topologyDesc.exeClass, conf)
        case TopologyDescriptionEntity.TYPE.DYNAMIC =>
          StormDynamicTopology.submit(topologyDesc.exeClass, conf)
        case m@_ =>
          throw new InvalidTopologyException("Unsupported topology type: " + topologyDesc.topoType)
      }
    } catch {
      case e: Throwable =>
        LOG.error(e.toString)
        ret = false
    }
    ret
  }

  override def stop(name: String, appConf: config.Config): Boolean = {
    var ret = true
    try {
      getNimbusClient(appConf).getClient.killTopology(name)
    } catch {
      case e: Throwable =>
        LOG.error(e.toString)
        ret = false
    }
    ret
  }

  override def status(name: String, appConf: config.Config): Boolean = {
    var ret = true
    try {
      getNimbusClient(appConf).getClient.getTopology(name)
    } catch {
      case e: Throwable =>
        LOG.error(e.toString)
        ret = false
    }
    ret
  }
}
