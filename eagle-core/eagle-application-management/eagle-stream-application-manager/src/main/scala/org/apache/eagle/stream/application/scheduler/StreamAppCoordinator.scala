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

import akka.actor.{Actor, ActorLogging, ActorRef, Props}

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
    case ClearPendingOperation =>
      commandLoader ! ClearPendingOperation
    case CommandLoaderEvent =>
      commandLoader ! CommandLoaderEvent
    case command: SchedulerCommand =>
      log.info(s"Executing command: $SchedulerCommand")
      commandExecutor ! command
    case HealthCheckerEvent =>
      commandExecutor ! HealthCheckerEvent
    case TerminatedEvent =>
      log.info("Coordinator exit ...")
      context.stop(self)
    case m@_ =>
      log.warning(s"Coordinator Unsupported message: $m")
  }
}
