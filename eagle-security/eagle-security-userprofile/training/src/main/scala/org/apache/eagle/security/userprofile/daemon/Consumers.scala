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

import akka.actor._
import org.apache.eagle.common.EagleExceptionWrapper
import org.apache.eagle.security.userprofile.model.ScheduleCommandEntity
import org.apache.eagle.security.userprofile.model.ScheduleCommandEntity.STATUS

import scala.util.{Failure, Success}

/**
 * @since  9/10/15
 */
abstract class CommandConsumer extends UntypedActor with ActorLogging{
  @volatile var dao:UserProfileCommandDao = null
  @volatile var config:SchedulerContext=null

  override def onReceive(message: Any): Unit = message match {
    case _config: SchedulerContext =>
      if(log.isDebugEnabled) {
        log.debug(s"Initialized with config: $config")
      } else {
        if(config == null) log.info(s"Initialized") else log.info("Reinitialized")
      }
      config = _config
      dao = new UserProfileCommandDao(_config.eagleServiceContext.serviceHost, _config.eagleServiceContext.servicePort, _config.eagleServiceContext.username, _config.eagleServiceContext.password, this)
    case _command: Command =>
      val _config = config
      if(_config == null) throw new IllegalStateException("Config is called before asigned")
      val _sender = sender()
      handle.apply(_command,_config,_sender)
    case _ => throw new IllegalArgumentException(s"Unknown message $message")
  }
  def handle: PartialFunction[(Command,SchedulerContext,ActorRef), Any]
}

class UserProfileCommandConsumer extends CommandConsumer{
  import context.dispatcher
  override def handle = {
    case (command, cfg, sender) => command match {
      case c:UserProfileCommand =>
        val entity = Command.asEntity(command)
        if(c.persistable) {
          dao.updateCommandStatus(entity, STATUS.EXECUTING, s"Command [$entity] is executing ") onComplete {  case _ => execute(c,entity,cfg) }
        }else{
          execute(c,entity,cfg)
        }
    }
  }

  private def execute(cmd: UserProfileCommand, entity:ScheduleCommandEntity,cfg: SchedulerContext): Unit ={
    cmd.execute(cfg) match {
      case Success(message) =>
        log.info(s"Executed successfully, returned: $message")
        if (cmd.persistable)
          dao.updateCommandStatus(entity, STATUS.SUCCEEDED, message) recover {
            case ex: Throwable => log.error(ex, s"Failed to update status of [$entity] as SUCCEEDED, due to exception" + ex.getMessage)
          }
      case Failure(ex) =>
        log.error(ex, s"Failed to execute: ${cmd.shell},due to: ${ex.getMessage}")
        if (cmd.persistable) {
          dao.updateCommandStatus(entity, STATUS.FAILED, EagleExceptionWrapper.wrap(ex.asInstanceOf[Exception])) recover {
            case ex: Throwable => log.error(ex, s"Failed to update status of [$entity]  as SUCCEEDED, due to exception: " + ex.getMessage)
        }
       }
    }
  }
}
