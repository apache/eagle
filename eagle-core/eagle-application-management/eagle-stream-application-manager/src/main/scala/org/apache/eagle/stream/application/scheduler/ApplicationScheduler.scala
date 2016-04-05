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

import akka.actor._
import com.typesafe.config.{Config, ConfigFactory, ConfigParseOptions, ConfigSyntax}
import org.apache.eagle.common.config.EagleConfigConstants
import org.apache.eagle.service.application.entity.TopologyExecutionEntity.TOPOLOGY_STATUS
import org.apache.eagle.service.application.entity.{TopologyExecutionEntity, TopologyOperationEntity}
import org.apache.eagle.service.application.entity.TopologyOperationEntity.{OPERATION, OPERATION_STATUS}
import org.apache.eagle.stream.application.ApplicationServiceDAO
import org.apache.eagle.stream.application.impl.StormApplicationManager
import org.apache.eagle.stream.application.model.{TopologyOperationModel, TopologyExecutionModel}

import scala.collection.JavaConversions
import scala.concurrent.duration._
import scala.util.{Failure, Success}


private[scheduler] class ScheduleEvent
private[scheduler] case class InitializationEvent(config: Config) extends ScheduleEvent
private[scheduler] case class TerminatedEvent() extends ScheduleEvent
private[scheduler] case class CommandLoaderEvent() extends ScheduleEvent
private[scheduler] case class HealthCheckerEvent() extends ScheduleEvent
private[scheduler] case class ResultCountEvent(value: Int) extends ScheduleEvent
private[scheduler] case class SchedulerCommand(executionModel: TopologyExecutionModel, operationModel:TopologyOperationModel) extends ScheduleEvent

case class EagleServiceUnavailableException(message:String) extends Exception(message)
case class DuplicatedDefinitionException(message:String) extends Exception(message)
case class LoadTopologyFailureException(message:String) extends Exception(message)

/**
 * 1. Sync command from eagle service
 * 2. Coordinate command to different actor
 * 3. Actor execute command as requested
 */
class ApplicationScheduler  {
  val config = ConfigFactory.load()

  def startDeamon(): Unit ={
    val thread = new Thread(new Runnable {
      override def run(): Unit = {
        ApplicationScheduler.this.start()
      }
    })
    thread.setDaemon(true)
    thread.start()
  }

  def start():Unit = {
    val system = ActorSystem("application-manager-scheduler", config)
    system.log.info(s"Started actor system: $system")

    import system.dispatcher

    val coordinator = system.actorOf(Props[StreamAppCoordinator])
    system.scheduler.scheduleOnce(0 seconds, coordinator, InitializationEvent(config))
    system.scheduler.schedule(1.seconds, 5.seconds, coordinator, CommandLoaderEvent)
    system.scheduler.schedule(600.seconds, 600.seconds, coordinator, HealthCheckerEvent)

    /*
     registerOnTermination is called when you have shut down the ActorSystem (system.shutdown),
     and the callbacks will be executed after all actors have been stopped.
     */
    system.registerOnTermination(new Runnable {
      override def run(): Unit = {
        coordinator ! TerminatedEvent
      }
    })
  }
}

private[scheduler] class StreamAppCoordinator extends Actor with ActorLogging {
  var commandLoader: ActorRef = null
  var commandExecutor: ActorRef = null

  override def preStart(): Unit = {
    commandLoader = context.actorOf(Props[AppCommandLoader], "command-loader")
    commandExecutor = context.actorOf(Props[AppCommandExecutor], "command-worker")
  }

  override def receive = {
    case InitializationEvent(config) => {
      log.info(s"Config updated: $config")
      commandLoader ! InitializationEvent(config)
      commandExecutor ! InitializationEvent(config)
    }
    case CommandLoaderEvent =>
      commandLoader ! CommandLoaderEvent
    case command: SchedulerCommand =>
      log.info(s"Executing comamnd: $SchedulerCommand")
      commandExecutor ! command
    case HealthCheckerEvent =>
      commandLoader ! HealthCheckerEvent
    case TerminatedEvent =>
      log.info("Coordinator exit ...")
      context.stop(self)
    case m@_ =>
      log.warning(s"Coordinator Unsupported message: $m")
  }
}

private[scheduler] class AppCommandLoader extends Actor with ActorLogging {
  @volatile var _config: Config = null
  @volatile var _dao: ApplicationServiceDAO = null

  import context.dispatcher

  override def receive = {
    case InitializationEvent(config: Config) =>
      _config = config
      _dao = new ApplicationServiceDAO(config, context.dispatcher)
    case CommandLoaderEvent => {
      val _sender = sender()
      _dao.readOperationsByStatus(OPERATION_STATUS.INITIALIZED) onComplete {
        case Success(optionalEntities) =>
          optionalEntities match {
            case Some(commands) => {
              log.info(s"Load ${commands.size()} new commands")
              JavaConversions.collectionAsScalaIterable(commands) foreach { command =>
                _dao.updateOperationStatus(command, OPERATION_STATUS.PENDING) onComplete {
                  case Success(response) =>
                    val operationModel = TopologyOperationEntity.toModel(command)
                    _dao.loadTopologyExecutionByName(operationModel.site, operationModel.application, operationModel.topology) onComplete {
                      case Success(optTopologyExecution) =>
                        optTopologyExecution match {
                          case Some(topologyExecution) => {
                            val topologyExecutionModel:TopologyExecutionModel = TopologyExecutionEntity.toModel(topologyExecution)
                            _sender ! SchedulerCommand(topologyExecutionModel, operationModel)
                          }
                          case None =>
                            log.error(s"Failed to find application definition with $operationModel")
                        }
                      case Failure(ex) =>
                        log.error(ex.getMessage)
                    }
                  case Failure(ex) =>
                    log.error(s"Got an exception to update command $command: ${ex.getMessage}")
                }
              }
            }
            case None =>
              log.info("Load 0 new commands")
          }
        case Failure(ex) =>
          log.error(s"Failed to get commands due to exception ${ex.getMessage}")
      }
    }
    case HealthCheckerEvent =>
      val _sender = sender()
      _dao.loadAllTopologyExecutionEntities() onComplete {
        case Success(optionalEntities) =>
          optionalEntities match {
            case Some(topologies) =>
              log.info(s"Load ${topologies.size()} new commands")
              JavaConversions.collectionAsScalaIterable(topologies) foreach { topology =>
                val topologyExecutionModel = TopologyExecutionEntity.toModel(topology)
                val checkStatusOperation = new TopologyOperationModel(topologyExecutionModel.site, topologyExecutionModel.application, java.util.UUID.randomUUID.toString, TopologyOperationEntity.OPERATION.STATUS, topologyExecutionModel.topology, OPERATION_STATUS.INITIALIZED, System.currentTimeMillis())
                _sender ! SchedulerCommand(topologyExecutionModel, checkStatusOperation)
              }
            case None =>
              log.info("Load 0 new commands")
          }
        case Failure(ex) =>
          log.error(s"Failed to get commands due to exception ${ex.getMessage}")
      }
    case TerminatedEvent =>
      context.stop(self)
    case m@_ => throw new UnsupportedOperationException(s"Event is not supported $m")
  }
}


private[scheduler] class AppCommandExecutor extends Actor with ActorLogging {
  @volatile var _config: Config = _
  @volatile var _dao: ApplicationServiceDAO = _
  @volatile var _streamAppManager = new StormApplicationManager

  import context.dispatcher

  def updateStatus(operationModel: TopologyOperationModel, executionModel: TopologyExecutionModel, ret: Boolean, nextState: String): Unit = {
    ret match {
      case true =>
        _dao.updateOperationStatus(TopologyOperationEntity.fromModel(operationModel), OPERATION_STATUS.SUCCESS)
        _dao.updateTopologyExecutionStatus(TopologyExecutionEntity.fromModel(executionModel), nextState)
      case _ =>
        _dao.updateOperationStatus(TopologyOperationEntity.fromModel(operationModel), OPERATION_STATUS.FAILED)
        _dao.updateTopologyExecutionStatus(TopologyExecutionEntity.fromModel(executionModel), nextState)
    }
  }

  def generateTopologyFullName(topologyExecutionModel: TopologyExecutionModel) = {
    val fullName = "eagle-%s-%s-%s".format(topologyExecutionModel.site, topologyExecutionModel.application, topologyExecutionModel.topology)
    fullName
  }

  def execute(operationModel: TopologyOperationModel, executionModel: TopologyExecutionModel): Unit = {
    var ret = true
    var nextState = TOPOLOGY_STATUS.STARTED
    val options: ConfigParseOptions = ConfigParseOptions.defaults.setSyntax(ConfigSyntax.PROPERTIES).setAllowMissing(false)

    operationModel.operation match {
      case OPERATION.START =>
        _dao.loadTopologyDescriptionByName(operationModel.site, operationModel.application, operationModel.topology) onComplete {
          case Success(optionalTopology) =>
            optionalTopology match {
              case Some(topology) =>
                val topologyConfig: Config = ConfigFactory.parseString(topology.config, options)
                if(topologyConfig.hasPath(EagleConfigConstants.APP_CONFIG)) {
                  val config = topologyConfig.getConfig(EagleConfigConstants.APP_CONFIG).withFallback(_config)
                  executionModel.fullName = generateTopologyFullName(executionModel)
                  val extConfig = ConfigFactory.parseString("envContextConfig.topologyName=" + executionModel.fullName)
                  val appConfig = extConfig.withFallback(config)
                  ret = _streamAppManager.start(topology, appConfig)
                  nextState = if(ret) TOPOLOGY_STATUS.STARTED else TOPOLOGY_STATUS.STOPPED
                  updateStatus(operationModel, executionModel, ret, nextState)
                }
              case None =>
                log.error(s"load 0 topologies with site=${operationModel.site} and application=${operationModel.application}")
            }
          case Failure(ex) =>
            log.error(s"Fail to load topology with site=${operationModel.site} and application=${operationModel.application}")
        }
      case OPERATION.STOP =>
        ret = _streamAppManager.stop(generateTopologyFullName(executionModel), _config)
        nextState = if(ret) TOPOLOGY_STATUS.STOPPED else TOPOLOGY_STATUS.STARTED
        updateStatus(operationModel, executionModel, ret, nextState)
      case OPERATION.STATUS =>
        ret = _streamAppManager.status(generateTopologyFullName(executionModel), _config)
        nextState = if(ret) TOPOLOGY_STATUS.STARTED else TOPOLOGY_STATUS.STOPPED
        updateStatus(operationModel, executionModel, ret, nextState)
      case m@_ =>
        log.warning("Unsupported operation: " + operationModel)
        return
    }
  }

  override def receive = {
    case InitializationEvent(config: Config) =>
      _config = config
      _dao = new ApplicationServiceDAO(config, context.dispatcher)

    case SchedulerCommand(executionModel, operationModel) =>
      execute(operationModel, executionModel)

    case m@_ =>
      log.warning("Unsupported operation $m")
  }

}


object ApplicationScheduler extends App {
  new ApplicationScheduler().start()
}

