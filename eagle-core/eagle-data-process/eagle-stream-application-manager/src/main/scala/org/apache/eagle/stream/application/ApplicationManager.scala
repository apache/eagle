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

package org.apache.eagle.stream.application

import com.typesafe.config.Config
import org.apache.eagle.stream.application.entity.TopologyOperationEntity.OPERATION
import org.apache.eagle.stream.application.entity.TopologyExecutionEntity.TOPOLOGY_STATUS
import org.apache.eagle.stream.application.model.{TopologyDescriptionModel, TopologyExecutionModel}
import org.slf4j.LoggerFactory


trait ApplicationManager {

  val LOG = LoggerFactory.getLogger(classOf[ApplicationManager])

  def execute(operation: String, topology: TopologyDescriptionModel, topologyExecution: TopologyExecutionModel, config: Config) = {
    var ret = true
    var nextState = TOPOLOGY_STATUS.STARTED
    operation match {
      case OPERATION.START =>
        ret = start(topology, config)
        nextState = if(ret) TOPOLOGY_STATUS.STARTED else TOPOLOGY_STATUS.STOPPED
      case OPERATION.STOP =>
        ret = stop(topologyExecution, config)
        nextState = if(ret) TOPOLOGY_STATUS.STOPPED else TOPOLOGY_STATUS.STARTED
      case OPERATION.STATUS =>
        ret = status(topologyExecution,config)
        nextState = if(ret) TOPOLOGY_STATUS.STARTED else TOPOLOGY_STATUS.STOPPED
      case m@_ =>
        LOG.warn("Unsupported operation: " + operation)
        ret = false
    }
    (ret, nextState)
  }

  def start(topologyDesc: TopologyDescriptionModel, config: Config): Boolean
  def stop(topologyExecution: TopologyExecutionModel, config: Config): Boolean
  def status(topologyExecution: TopologyExecutionModel, config: Config): Boolean
}
