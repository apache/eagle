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
package eagle.security.userprofile.daemon

import akka.actor.{Props, _}

/**
 * @since  9/11/15
 */
case class Initialized(context: SchedulerContext)
case class DynamicLoadDataSourceConfig()
case class CheckPersistedCommands(var site: String, var category: SchedulerContext.COMMAND_TYPE.TYPE)
case class CheckScheduledStatus(var site: String, var category: SchedulerContext.COMMAND_TYPE.TYPE)
case class Terminated(context: SchedulerContext)

class CommandCoordinator extends UntypedActor with ActorLogging{
  var consumer:ActorRef = null
  var persistedCommandsProducer:ActorRef = null
  var scheduledCommandsProducer:ActorRef = null

  override def preStart(): Unit = {
    consumer = context.actorOf(Props[UserProfileCommandConsumer],"userprofile-command-consumer")
    persistedCommandsProducer = context.actorOf(Props[PersistedCommandProducer],"persisted-command-producer")
    scheduledCommandsProducer = context.actorOf(Props[ScheduledCommandProducer],"scheduled-command-producer")
  }

  override def onReceive(message: Any): Unit = message match {
    case Initialized(config) =>
      log.info(s"Config updated: $config")
      persistedCommandsProducer ! config
      scheduledCommandsProducer ! config
      consumer ! config
    case request: CheckPersistedCommands =>
      persistedCommandsProducer ! request
    case request: CheckScheduledStatus =>
      scheduledCommandsProducer ! request
    case command: Command =>
      log.info(s"*** NEW Command: $command ***")
      consumer ! command
    case DynamicLoadDataSourceConfig =>
    // TODO: Dynamically load data source config
    case Terminated(config) =>
      log.info("Coordinator exited")
    case _ =>
      log.warning(s"Unhandled message: $message")
  }
}