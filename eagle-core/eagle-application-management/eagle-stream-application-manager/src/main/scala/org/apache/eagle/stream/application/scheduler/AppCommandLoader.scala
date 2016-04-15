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

import akka.actor.{Actor, ActorLogging}
import com.typesafe.config.Config
import org.apache.eagle.service.application.entity.TopologyOperationEntity.OPERATION_STATUS
import org.apache.eagle.stream.application.ApplicationSchedulerAsyncDAO

import scala.collection.JavaConversions
import scala.util.{Failure, Success}


private[scheduler] class AppCommandLoader extends Actor with ActorLogging {
  @volatile var _config: Config = null
  @volatile var _dao: ApplicationSchedulerAsyncDAO = null

  import context.dispatcher

  override def receive = {
    case InitializationEvent(config: Config) =>
      _config = config
      _dao = new ApplicationSchedulerAsyncDAO(config, context.dispatcher)
    case ClearPendingOperation =>
      if(_dao == null) _dao = new ApplicationSchedulerAsyncDAO(_config, context.dispatcher)
      _dao.clearPendingOperations()
    case CommandLoaderEvent => {
      val _sender = sender()
      _dao.readOperationsByStatus(OPERATION_STATUS.INITIALIZED) onComplete {
        case Success(commands) => {
          log.info(s"Load ${commands.size()} new commands")
          JavaConversions.collectionAsScalaIterable(commands) foreach { command =>
            command.setStatus(OPERATION_STATUS.PENDING)
            _dao.updateOperationStatus(command) onComplete {
              case Success(response) =>
                _dao.loadTopologyExecutionByName(command.getSite, command.getApplication, command.getTopology) onComplete {
                  case Success(topologyExecution) => {
                    _sender ! SchedulerCommand(topologyExecution, command)
                  }
                  case Failure(ex) =>
                    log.error(ex.getMessage)
                    command.setMessage(ex.getMessage)
                    command.setStatus(OPERATION_STATUS.FAILED)
                    _dao.updateOperationStatus(command)
                }
              case Failure(ex) =>
                log.error(s"Got an exception to update command $command: ${ex.getMessage}")
                command.setMessage(ex.getMessage)
                command.setStatus(OPERATION_STATUS.FAILED)
                _dao.updateOperationStatus(command)
            }
          }
        }
        case Failure(ex) =>
          log.error(s"Failed to get commands due to exception ${ex.getMessage}")
      }
    }
    case TerminatedEvent =>
      context.stop(self)
    case m@_ => throw new UnsupportedOperationException(s"Event is not supported $m")
  }
}
