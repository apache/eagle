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

import java.util

import akka.actor._
import akka.routing.RoundRobinRouter
import org.apache.eagle.stream.dsl.dao.{AppEntityDaoImpl}

import org.apache.eagle.stream.dsl.entity.{AppDefinitionEntity, AppCommandEntity}
import org.apache.eagle.stream.dsl.StreamBuilder._
import org.slf4j.LoggerFactory
import scala.collection.JavaConverters._


object StreamAppConstants {
  val SCHEDULE_SYSTEM = "stream-app-scheduler"
  val SCHEDULE_INTERVAL = 1000
  val SCHEDULE_NUM_WORKERS = 3
  val HOST = "localhost"
  val PORT = 9098
  val SERVICE_NAME = "admin"
  val SERVICE_PASSWD = "secret"
}

private[execution] class ScheduleEvent
private[execution] case class InitializationEvent() extends ScheduleEvent
private[execution] case class TerminatedEvent() extends ScheduleEvent
private[execution] case class CommandLoaderEvent() extends ScheduleEvent
private[execution] case class HealthCheckerEvent() extends ScheduleEvent

private[execution] class Command
private[execution] case class StartCommand(appCommandEntity: AppCommandEntity) extends Command
private[execution] case class StopCommand(appCommandEntity: AppCommandEntity) extends Command
private[execution] case class RestartCommand(appCommandEntity: AppCommandEntity) extends Command

/**
 * 1. Sync command from eagle service
 * 2. Coordinate command to different actor
 * 3. Actor execute command as requested
 */
private[execution] class StreamAppScheduler {
  private val logger = LoggerFactory.getLogger(classOf[StreamAppScheduler])

  def start():Unit = {
    val system = ActorSystem(StreamAppConstants.SCHEDULE_SYSTEM)
    system.log.info(s"Started actor system: $system")

    val coordinator = system.actorOf(Props[StreamAppCoordinator])
    //coordinator ! InitializationEvent()
    coordinator ! CommandLoaderEvent
    //system.scheduler.schedule(3.seconds,StreamAppConstants.SCHEDULE_INTERVAL.seconds, coordinator, CommandLoaderEvent())

    system.registerOnTermination(new Runnable {
      override def run(): Unit = {
        coordinator ! TerminatedEvent()
      }
    })
  }

}


private[execution] class StreamAppCoordinator extends Actor with ActorLogging {
  private val loader:ActorRef = context.actorOf(Props[StreamAppCommandLoader],"command-loader")

  override def receive = {
    case InitializationEvent =>
      log.info("Initialization Event")
    case CommandLoaderEvent =>
      loader ! CommandLoaderEvent
    case TerminatedEvent =>
      context.stop(self)
    case m@_ => throw new UnsupportedOperationException(s"Event not supported $m")
  }
}


private[execution] class StreamAppCommandLoader extends Actor with ActorLogging {

  def loadCommands(): util.List[AppCommandEntity] = {
    val dao = new AppEntityDaoImpl(StreamAppConstants.HOST, StreamAppConstants.PORT, StreamAppConstants.SERVICE_NAME, StreamAppConstants.SERVICE_PASSWD)
    val query = "AppCommandService[@status=\"UNKNOWN\"]{*}"
    println(query)
    val result = dao.search(query, Int.MaxValue)
    result.getObj().asInstanceOf[util.List[AppCommandEntity]]
  }

  val workerRouter = context.actorOf(
    Props[StreamAppCommandExecutor].withRouter(RoundRobinRouter(StreamAppConstants.SCHEDULE_NUM_WORKERS)), name = "command-executor")

  override def receive = {
    case CommandLoaderEvent =>
      val commands = loadCommands()
      if(commands == null) {
        println("cannot load any commands")
        throw new Exception("cannot load any commands")
      }

      for(command <- commands.asScala) {
        command.getTags.get("type") match {
          case AppCommandEntity.Type.START =>
            workerRouter ! StartCommand(command)
          case AppCommandEntity.Type.STOP =>
            workerRouter ! StopCommand(command)
          case AppCommandEntity.Type.RESTART =>
            workerRouter ! RestartCommand(command)
          case m@_ => throw new UnsupportedOperationException(s"Event not supported $m")
        }
      }
    case m@_ => throw new UnsupportedOperationException(s"Event not supported $m")
  }
}

private[execution] class StreamAppCommandExecutor extends Actor with ActorLogging {

  def loadAppDefinition(command: AppCommandEntity): AppDefinitionEntity = {
    val dao = new AppEntityDaoImpl(StreamAppConstants.HOST, StreamAppConstants.PORT, StreamAppConstants.SERVICE_NAME, StreamAppConstants.SERVICE_PASSWD)
    val appName = command.getAppName()
    val site = command.getTags().get("site")
    val query = "AppDefinitionService[@name=\"%s\" AND @site=\"%s\"]{*}".format(appName, site)
    println(query)
    val result = dao.search(query, Integer.MAX_VALUE)
    log.debug(result.toString())
    if(result.getObj == null || result.getObj.size() == 0) {
      println("Fail to find AppDefnition by appName")
      throw new Exception("Fail to find AppDefnition by appName" + appName)
    }
    result.getObj().get(0).asInstanceOf[AppDefinitionEntity]
  }

  override def receive = {
    case StartCommand(command) => {
      try {
        val appDefinition = loadAppDefinition(command)
        val code = appDefinition.getDefinition().stripMargin
        //val env = appDefinition.getExecutionEnvironment()
        val ret = StreamEvaluator(code).evaluate[storm]
        println(ret)
      } catch {
        case e:Throwable => {
          log.error(s"Failed to parse load App definition",e)
          throw ParseException(s"Failed load App definition",e)
        }
      }
    }
    case StopCommand(command) =>
      println("StopCommand")
    case RestartCommand(command) =>
      println("RestartCommand")
    case m@_ => throw new UnsupportedOperationException(s"Event not supported $m")
  }
}

private[execution] class StreamAppHealthChecker extends Actor {
  override def receive: Actor.Receive = ???
}

object StreamAppScheduler extends App {
  new StreamAppScheduler().start()
}
