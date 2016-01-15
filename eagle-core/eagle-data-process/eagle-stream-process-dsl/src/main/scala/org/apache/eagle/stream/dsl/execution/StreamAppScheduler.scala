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
import akka.routing.RoundRobinRouter
import com.typesafe.config.ConfigFactory
import org.apache.eagle.log.entity.GenericServiceAPIResponseEntity
import org.apache.eagle.stream.dsl.dao.AppEntityDaoImpl

import org.apache.eagle.stream.dsl.entity.{AppDefinitionEntity, AppCommandEntity}
import org.apache.eagle.stream.dsl.StreamBuilder._
import org.apache.eagle.stream.dsl.rest.AppConstants
import scala.collection.JavaConverters._

object StreamAppConstants {
  val SCHEDULE_SYSTEM = "stream-app-scheduler"
  val SCHEDULE_INTERVAL = 1000
  val SCHEDULE_NUM_WORKERS = 3
  val SERVICE_TIMEOUT = 5
  val HOST = "localhost"
  val PORT = 9098
  val SERVICE_NAME = "admin"
  val SERVICE_PASSWD = "secret"
}

private[execution] class ScheduleEvent
private[execution] case class InitializationEvent() extends ScheduleEvent
private[execution] case class TerminatedEvent() extends ScheduleEvent
private[execution] case class CommandLoaderEvent() extends ScheduleEvent
private[execution] case class CommandExecuteEvent(appCommandEntity: AppCommandEntity) extends ScheduleEvent
private[execution] case class HealthCheckerEvent() extends ScheduleEvent
private[execution] case class ResultCountEvent(value: Int) extends ScheduleEvent

case class EagleServiceUnavailableException(message:String) extends Exception(message)
case class DuplicatedDefinitionException(message:String) extends Exception(message)


/**
 * 1. Sync command from eagle service
 * 2. Coordinate command to different actor
 * 3. Actor execute command as requested
 */
private[execution] class StreamAppScheduler(conf: String) {

  val config = ConfigFactory.parseString(conf)

  def start():Unit = {
    val system = ActorSystem(StreamAppConstants.SCHEDULE_SYSTEM, config)
    system.log.info(s"Started actor system: $system")

    val coordinator = system.actorOf(Props[StreamAppCoordinator])
    coordinator ! InitializationEvent

  }
}

private[execution] class StreamAppCoordinator extends Actor with ActorLogging {
  private val loader: ActorRef = context.actorOf(Props[StreamAppCommandLoader], "command-loader")

  override def receive = {
    case InitializationEvent => {
      loader ! InitializationEvent
      loader ! CommandLoaderEvent
    }

    case TerminatedEvent =>
      context.stop(self)

    case m@_ => throw new UnsupportedOperationException(s"Event not supported $m")
  }
}

private[execution] class StreamAppCommandLoader extends Actor with ActorLogging {

  def loadCommands(): GenericServiceAPIResponseEntity[_] = {
    val dao = new AppEntityDaoImpl(StreamAppConstants.HOST, StreamAppConstants.PORT, StreamAppConstants.SERVICE_NAME, StreamAppConstants.SERVICE_PASSWD)
    val query = "AppCommandService[@status=\"%s\"]{*}".format(AppCommandEntity.Status.INITIALIZED)
    dao.search(query, Int.MaxValue)
  }

  var progressListener: Option[ActorRef] = None
  val workerRouter = context.actorOf(
    Props[StreamAppCommandExecutor].withRouter(RoundRobinRouter(StreamAppConstants.SCHEDULE_NUM_WORKERS)), name = "command-executor")

  override def receive = {
    case InitializationEvent if progressListener.isEmpty =>
      progressListener = Some(sender())

    case CommandLoaderEvent => {
      val response = loadCommands()
      //println("response" + response.getObj.toString)
      if(response.getException != null)
        throw new EagleServiceUnavailableException(s"Service is unavailable" + response.getException)

      val commands = response.getObj().asScala
      if(commands != null && commands.size != 0){
        val appCommands = commands.toList
        for(command <- appCommands) {
          val cmd = command.asInstanceOf[AppCommandEntity]
          cmd.setStatus(AppCommandEntity.Status.PENDING)
          workerRouter ! CommandExecuteEvent(cmd)
        }
      }
    }

    case TerminatedEvent =>
      context.stop(self)

    case m@_ => throw new UnsupportedOperationException(s"Event not supported $m")
  }
}

private[execution] class StreamAppCommandExecutor extends Actor with ActorLogging {
  val dao = new AppEntityDaoImpl(StreamAppConstants.HOST, StreamAppConstants.PORT, StreamAppConstants.SERVICE_NAME, StreamAppConstants.SERVICE_PASSWD)

  def loadAppDefinition(appName: String, site: String): GenericServiceAPIResponseEntity[_] = {
    val query = "AppDefinitionService[@name=\"%s\" AND @site=\"%s\"]{*}".format(appName, site)
    println(query)
    dao.search(query, Integer.MAX_VALUE)
  }

  def changeAppStatus(app: AppDefinitionEntity, newStatus: String): Unit = {
    app.setExecutionStatus(newStatus)
    dao.update(app, AppConstants.APP_DEFINITION_SERVICE)
  }

  def changeCommandStatus(cmd: AppCommandEntity, newStatus: String): Unit = {
    cmd.setStatus(newStatus)
    dao.update(cmd, AppConstants.APP_COMMAND_SERVICE)
  }

  def startCommand(appDefinition: AppDefinitionEntity, appCommand: AppCommandEntity) {
    val code = appDefinition.getDefinition.stripMargin
    var newAppStatus: String = AppDefinitionEntity.STATUS.UNKNOWN
    var newCmdStatus: String = AppCommandEntity.Status.PENDING
    try {
      changeAppStatus(appDefinition, AppDefinitionEntity.STATUS.STARTING)
      val ret = StreamEvaluator(code).evaluate[storm]

      ret match {
        case true => {
          newAppStatus = AppDefinitionEntity.STATUS.RUNNING
          newCmdStatus = AppCommandEntity.Status.RUNNING
        }
        case m@_ => {
          newAppStatus = AppDefinitionEntity.STATUS.STOPPED
          newCmdStatus = AppCommandEntity.Status.DOWN
        }
      }
    } catch {
      case e: Throwable => {
        newAppStatus = AppDefinitionEntity.STATUS.STOPPED
        newCmdStatus = AppCommandEntity.Status.DOWN
      }
    }
    changeAppStatus(appDefinition, newAppStatus)
    changeCommandStatus(appCommand, newCmdStatus)

  }

  def stopCommand(appDefinition: AppDefinitionEntity, command: AppCommandEntity): Unit = {
    //TODO
  }

  def restartCommand(appDefinition: AppDefinitionEntity, command: AppCommandEntity): Unit = {
    //TODO
  }

  override def receive = {
    case CommandExecuteEvent(command) => {
      val appName = command.getAppName
      val site = command.getTags().get("site")
      val result = loadAppDefinition(appName, site)
      println("result=" + result.getObj.toString)

      if(result.getException != null) {
        throw new EagleServiceUnavailableException("Service is not available" + result.getException)
      }

      val definitions = result.getObj()
      if(definitions.size != 1) {
        log.error(s"The AppDefinitionService result searched by name $appName is not correct")
        throw new DuplicatedDefinitionException(s"The AppDefinitionService result searched by name $appName is not correct")
      }

      val appDefinition = definitions.get(0).asInstanceOf[AppDefinitionEntity]
      command.getTags.get("type") match {
        case AppCommandEntity.Type.START => {
          startCommand(appDefinition, command)
        }
        case AppCommandEntity.Type.STOP => {
          stopCommand(appDefinition, command)
        }
        case AppCommandEntity.Type.RESTART => {
          restartCommand(appDefinition, command)
        }
        case m@_ =>
          log.warning("Unsupported operation $m")
      }
    }

    case m@_ =>
      log.warning("Unsupported operation $m")
  }
}


object StreamAppScheduler extends App {
  val conf: String = """
                          akka.loglevel = "DEBUG"
                          akka.actor.debug {
                            receive = on
                            lifecycle = on
                          }
                     """
  new StreamAppScheduler(conf).start()
}
