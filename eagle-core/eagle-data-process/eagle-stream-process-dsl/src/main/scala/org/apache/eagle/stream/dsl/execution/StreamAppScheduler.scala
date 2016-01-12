/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.eagle.stream.dsl.execution

import akka.actor._
import scala.concurrent.duration._

object StreamAppConstants {
  val SCHEDULE_SYSTEM = "stream-app-scheduler"
  val SCHEDULE_INTERVAL = 1000
}

private[execution] class ScheduleEvent
private[execution] case class InitializationEvent() extends ScheduleEvent
private[execution] case class TerminatedEvent() extends ScheduleEvent
private[execution] case class CommandLoaderEvent() extends ScheduleEvent
private[execution] case class HealthCheckerEvent() extends ScheduleEvent

private[execution] abstract class Command

/**
 * 1. Sync command from eagle service
 * 2. Coordinate command to different actor
 * 3. Actor execute command as requested
 */
private[execution] class StreamAppScheduler {
  def start():Unit = {
    val system = ActorSystem(StreamAppConstants.SCHEDULE_SYSTEM)
    system.log.info(s"Started actor system: $system")

    import system.dispatcher

    val coordinator = system.actorOf(Props[StreamAppCoordinator])
    system.scheduler.scheduleOnce(0.seconds, coordinator,InitializationEvent())
    system.scheduler.schedule(1.seconds,StreamAppConstants.SCHEDULE_INTERVAL.seconds,coordinator,CommandLoaderEvent())
    system.registerOnTermination(new Runnable {
      override def run(): Unit = {
        coordinator ! TerminatedEvent()
      }
    })
  }
}

private[execution] class StreamAppCoordinator  extends UntypedActor with ActorLogging{
  private var loader:ActorRef = null
  private var executor:ActorRef = null

  override def preStart(): Unit = {
    loader = context.actorOf(Props[StreamAppCommandLoader],"command-loader")
    executor = context.actorOf(Props[StreamAppCommandExecutor],"command-executor")
  }

  override def onReceive(message: Any): Unit = message match {
    case InitializationEvent =>
      // init scheduler
    case CommandLoaderEvent =>
      // load command
    case HealthCheckerEvent =>
      // check health
    case command:Command =>
      // execute command
      executor ! command
    case m@_ => throw new UnsupportedOperationException(s"Event not supported $m")
  }
}

private[execution] class StreamAppCommandLoader extends Actor {
  override def receive: Actor.Receive = ???
}

private[execution] class StreamAppCommandExecutor extends Actor{
  override def receive: Receive = {
    case _ =>
  }
}

private[execution] class StreamAppHealthChecker extends Actor {
  override def receive: Actor.Receive = ???
}

object StreamAppScheduler extends App{
  new StreamAppScheduler().start()
}