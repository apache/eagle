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
package org.apache.eagle.security.userprofile.daemon

import akka.actor.{ActorSystem, Props}
import SchedulerContext.COMMAND_TYPE
import org.apache.eagle.dataproc.util.ConfigOptionParser

import scala.concurrent.duration._

/**
 * User Profile Training Scheduler
 *
 * @since  0.3.0
 */
class Scheduler(config:SchedulerContext) {
  /**
   * Start Daemon Scheduler
   */
  def start(): Unit = {
    val system = ActorSystem(SchedulerContext.SchedulerSystem)

    system.log.info(s"Started actor system: $system")

    import system.dispatcher

    val coordinator = system.actorOf(Props[CommandCoordinator])
    // Initialize when start
    system.scheduler.scheduleOnce(0.seconds,coordinator,Initialized(config))

    system.scheduler.schedule(1.seconds,config.syncIntervalSeconds.seconds,coordinator,CheckOndemandTrainingStatus(config.site,COMMAND_TYPE.USER_PROFILE_TRAINING))

    system.scheduler.schedule(config.trainingInitialDelaySeconds.seconds,config.trainingIntervalSeconds.seconds,coordinator,CheckPeriodicTrainingStatus(config.site,COMMAND_TYPE.USER_PROFILE_TRAINING))

    system.registerOnTermination(new Runnable {
      override def run(): Unit = {
        coordinator ! Terminated(config)
      }
    })
  }
}

object Scheduler{
  def main(args:Array[String]): Unit ={
    new ConfigOptionParser().load(args)
    new Scheduler(SchedulerContext.load).start()
  }
}