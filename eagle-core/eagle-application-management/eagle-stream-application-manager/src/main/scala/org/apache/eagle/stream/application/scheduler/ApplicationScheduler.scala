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

import akka.actor.{ActorSystem, Props}
import com.typesafe.config.Config
import org.apache.eagle.service.application.AppManagerConstants
import org.apache.eagle.service.application.entity.{TopologyExecutionEntity, TopologyOperationEntity}

import scala.concurrent.duration._


private[scheduler] class ScheduleEvent
private[scheduler] case class InitializationEvent(config: Config) extends ScheduleEvent
private[scheduler] case class TerminatedEvent() extends ScheduleEvent
private[scheduler] case class CommandLoaderEvent() extends ScheduleEvent
private[scheduler] case class HealthCheckerEvent() extends ScheduleEvent
private[scheduler] case class ClearPendingOperation() extends ScheduleEvent
private[scheduler] case class SchedulerCommand(topologyExecution: TopologyExecutionEntity, topologyOperation: TopologyOperationEntity) extends ScheduleEvent

case class EagleServiceUnavailableException(message:String) extends Exception(message)
case class DuplicatedDefinitionException(message:String) extends Exception(message)
case class LoadTopologyFailureException(message:String) extends Exception(message)


/**
 * 1. Sync command from eagle service
 * 2. Coordinate command to different actor
 * 3. Actor execute command as requested
 */
class ApplicationScheduler {
  //val config = ConfigFactory.load()
  val DEFAULT_COMMAND_LOADER_INTERVAL = 2
  val DEFAULT_HEALTH_CHECK_INTERVAL = 5

  def start(config: Config) = {
    val system = ActorSystem("application-manager-scheduler", config)
    system.log.info(s"Started actor system: $system")

    import system.dispatcher

    val commandLoaderInterval: Long = if(config.hasPath(AppManagerConstants.APP_COMMAND_LOADER_INTERVAL)) config.getLong(AppManagerConstants.APP_COMMAND_LOADER_INTERVAL) else DEFAULT_COMMAND_LOADER_INTERVAL
    val healthCheckInterval: Long = if(config.hasPath(AppManagerConstants.APP_HEALTH_CHECK_INTERVAL)) config.getLong(AppManagerConstants.APP_HEALTH_CHECK_INTERVAL) else DEFAULT_HEALTH_CHECK_INTERVAL

    val coordinator = system.actorOf(Props[StreamAppCoordinator])
    system.scheduler.scheduleOnce(0 seconds, coordinator, InitializationEvent(config))
    system.scheduler.scheduleOnce(1 seconds, coordinator, ClearPendingOperation)
    system.scheduler.schedule(2.seconds, commandLoaderInterval.seconds, coordinator, CommandLoaderEvent)
    system.scheduler.schedule(10.seconds, healthCheckInterval.seconds, coordinator, HealthCheckerEvent)

    /*
     registerOnTermination is called when you have shut down the ActorSystem (system.shutdown),
     and the callbacks will be executed after all actors have been stopped.
     */
    system.registerOnTermination(new Runnable {
      override def run(): Unit = {
        coordinator ! TerminatedEvent
      }
    })
    system
  }
}

