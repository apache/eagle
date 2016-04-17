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

package org.apache.eagle.stream.application.scheduler

import java.util.concurrent.Callable

import akka.actor.{Actor, ActorLogging}
import akka.dispatch.Futures
import com.typesafe.config.{Config, ConfigFactory, ConfigParseOptions, ConfigSyntax}
import org.apache.eagle.common.config.EagleConfigConstants
import org.apache.eagle.service.application.AppManagerConstants
import org.apache.eagle.service.application.entity.TopologyOperationEntity.OPERATION
import org.apache.eagle.service.application.entity.{TopologyExecutionEntity, TopologyExecutionStatus, TopologyOperationEntity}
import org.apache.eagle.stream.application.{ApplicationManager, ApplicationSchedulerAsyncDAO, ExecutionPlatformFactory}

import scala.collection.JavaConversions
import scala.util.{Failure, Success}


private[scheduler] class AppCommandExecutor extends Actor with ActorLogging {
  @volatile var _config: Config = _
  @volatile var _dao: ApplicationSchedulerAsyncDAO = _

  import context.dispatcher

  def start(topologyExecution: TopologyExecutionEntity, topologyOperation: TopologyOperationEntity) = {
    val options: ConfigParseOptions = ConfigParseOptions.defaults.setSyntax(ConfigSyntax.PROPERTIES).setAllowMissing(false)
    _dao.loadTopologyDescriptionByName(topologyOperation.getSite, topologyOperation.getApplication, topologyOperation.getTopology) onComplete {
      case Success(topology) =>
        val topologyConfig: Config = ConfigFactory.parseString(topology.getContext, options)

        if(!topologyConfig.hasPath(EagleConfigConstants.APP_CONFIG)) {
          throw new Exception("Fail to detect topology configuration")
        }
        val config = topologyConfig.getConfig(EagleConfigConstants.APP_CONFIG).withFallback(_config)
        val clusterType = if(config.hasPath(AppManagerConstants.CLUSTER_ENV)) config.getString(AppManagerConstants.CLUSTER_ENV) else AppManagerConstants.EAGLE_CLUSTER_STORM
        topologyExecution.setEnvironment(clusterType)

        Futures.future(new Callable[TopologyExecutionEntity]{
          override def call(): TopologyExecutionEntity = {
            topologyExecution.setStatus(TopologyExecutionStatus.STARTING)
            _dao.updateTopologyExecutionStatus(topologyExecution)
            ExecutionPlatformFactory.getApplicationManager(clusterType).start(topology, topologyExecution, config)
            topologyOperation.setStatus(TopologyOperationEntity.OPERATION_STATUS.SUCCESS)
            topologyExecution
          }
        }, context.dispatcher) onComplete {
          case Success(topologyExecutionEntity) =>
            topologyExecution.setStatus(TopologyExecutionStatus.STARTED)
            topologyOperation.setStatus(TopologyOperationEntity.OPERATION_STATUS.SUCCESS)
            updateStatus(topologyExecution, topologyOperation)
          case Failure(ex) =>
            topologyOperation.setMessage(ex.getMessage)
            topologyOperation.setStatus(TopologyOperationEntity.OPERATION_STATUS.FAILED)
            _dao.updateOperationStatus(topologyOperation)
        }

      case Failure(ex) =>
        topologyOperation.setMessage(ex.getMessage)
        topologyOperation.setStatus(TopologyOperationEntity.OPERATION_STATUS.FAILED)
        _dao.updateOperationStatus(topologyOperation)
    }
  }

  def stop(topologyExecution: TopologyExecutionEntity, topologyOperation: TopologyOperationEntity) = {
    val clusterType = topologyExecution.getEnvironment

    Futures.future(new Callable[TopologyExecutionEntity]{
      override def call(): TopologyExecutionEntity = {
        topologyExecution.setStatus(TopologyExecutionStatus.STOPPING)
        _dao.updateTopologyExecutionStatus(topologyExecution)
        ExecutionPlatformFactory.getApplicationManager(clusterType).stop(topologyExecution, _config)
        topologyOperation.setStatus(TopologyOperationEntity.OPERATION_STATUS.SUCCESS)
        topologyExecution
      }
    }, context.dispatcher) onComplete {
      case Success(topologyExecutionEntity) =>
        topologyExecution.setStatus(TopologyExecutionStatus.STOPPED)
        topologyOperation.setStatus(TopologyOperationEntity.OPERATION_STATUS.SUCCESS)
        updateStatus(topologyExecution, topologyOperation)
      case Failure(ex) =>
        topologyOperation.setMessage(ex.getMessage)
        topologyOperation.setStatus(TopologyOperationEntity.OPERATION_STATUS.FAILED)
        _dao.updateOperationStatus(topologyOperation)
    }
  }

  def status(topologyExecution: TopologyExecutionEntity) = {
    val clusterType = topologyExecution.getEnvironment

    Futures.future(new Callable[TopologyExecutionEntity]{
      override def call(): TopologyExecutionEntity = {
        ExecutionPlatformFactory.getApplicationManager(clusterType).status(topologyExecution, _config)
        topologyExecution
      }
    }, context.dispatcher) onComplete {
      case _ =>
        _dao.updateTopologyExecutionStatus(topologyExecution)
    }
  }

  def updateStatus(topologyExecution: TopologyExecutionEntity, topologyOperation: TopologyOperationEntity): Unit = {
    _dao.updateOperationStatus(topologyOperation)
    _dao.updateTopologyExecutionStatus(topologyExecution)
  }

  def execute(topologyExecution: TopologyExecutionEntity, topologyOperation: TopologyOperationEntity): Unit = {
    try {
      topologyOperation.getOperation match {
        case OPERATION.START =>
          start(topologyExecution, topologyOperation)
        case OPERATION.STOP =>
          stop(topologyExecution, topologyOperation)
        case m@_ =>
          log.warning("Unsupported operation: " + topologyOperation)
          throw new Exception("Unsupported operation: " + topologyOperation)
      }
    } catch {
      case e: Throwable =>
        topologyOperation.setMessage(e.getMessage)
        topologyOperation.setStatus(TopologyOperationEntity.OPERATION_STATUS.FAILED)
        _dao.updateOperationStatus(topologyOperation)
    }
  }

  override def receive = {
    case InitializationEvent(config: Config) =>
      _config = config
      _dao = new ApplicationSchedulerAsyncDAO(config, context.dispatcher)
    case SchedulerCommand(topologyExecution, topologyOperation) =>
      execute(topologyExecution, topologyOperation)
    case HealthCheckerEvent =>
      _dao.loadAllTopologyExecutionEntities() onComplete {
        case Success(topologyExecutions) =>
          log.info(s"Load ${topologyExecutions.size()} topologies in execution")
          JavaConversions.collectionAsScalaIterable(topologyExecutions) foreach { topologyExecution =>
            try{
              status(topologyExecution)
            } catch {
              case ex: Throwable =>
                log.error(ex.getMessage)
            }
          }
        case Failure(ex) =>
          log.error(s"Fail to load any topologyExecutionEntity due to Exception: ${ex.getMessage}")
      }
    case TerminatedEvent =>
      log.info("Going to shutdown executorService ...")
      ApplicationManager.executorService.shutdownNow()
      context.stop(self)
    case m@_ =>
      log.warning("Unsupported operation $m")
  }

}
