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

import akka.actor.{ActorLogging, ActorRef, UntypedActor}
import akka.event.LoggingAdapter
import org.apache.eagle.common.DateTimeUtil
import org.apache.eagle.security.userprofile.daemon.SchedulerContext.{COMMAND_SOURCE, COMMAND_TYPE}
import org.apache.eagle.security.userprofile.model.ScheduleCommandEntity
import org.apache.eagle.security.userprofile.model.ScheduleCommandEntity.STATUS

import scala.collection.JavaConversions
import scala.util.{Failure, Success}

/**
 * @since  9/10/15
 */

object CommandProducer{
  def toStateName(site:String,category:COMMAND_TYPE.TYPE):String = s"$site-${category.toString.toLowerCase}"

  def getProgramStatus(site:String, category:COMMAND_TYPE.TYPE,policy: SchedulePolicy):Option[Long] = policy.getStatus(toStateName(site,category))

  def checkProgramStatus(site:String, category:COMMAND_TYPE.TYPE,policy: SchedulePolicy,inputPath:String, sender: ActorRef, log: LoggingAdapter)(buildCommand: (Long,String) => UserProfileCommand): Unit = {
    val stateName = toStateName(site,category)
    var status: Long = policy.getStatus(stateName).getOrElse(0l)
    if (status > 0) {
      status = policy.formatStatus(status)
      if (log.isDebugEnabled) log.debug(s"State [$stateName] is found: [${DateTimeUtil.millisecondsToHumanDateWithSeconds(status)}]")
      if (!policy.validateStatus(status)) {
        if (log.isDebugEnabled) {
          val nextStatus = policy.nextStatus(status)
          log.debug(s"State [$stateName]: [${DateTimeUtil.millisecondsToHumanDateWithSeconds(status)}], will execute at [${DateTimeUtil.millisecondsToHumanDateWithSeconds(nextStatus)}]")
        }
        return
      }
    } else {
      status = policy.formatStatus(System.currentTimeMillis())
      log.info(s"Initialized State [$stateName] as current timestamp by default: $status")
      policy updateStatus(stateName, status)
      // return
    }

    val path = Utils.formatPathWithMilliseconds(inputPath)(status)
    log.info(s"Create a new command for [status: ${DateTimeUtil.millisecondsToHumanDateWithSeconds(status)}, path: $path]")
    sender ! buildCommand(status,path)
    val nextStatus = policy nextStatus status
    policy updateStatus(stateName, nextStatus)
  }
}

class OndemandTrainingProducer extends UntypedActor with ActorLogging{
  @volatile var dao:UserProfileCommandDao=null
  @volatile var config:SchedulerContext=null

  import context.dispatcher

  override def onReceive(message: Any): Unit = message match {
    case _config: SchedulerContext =>
      if(log.isDebugEnabled) {
        log.debug(s"Initialized with config: ${_config}")
      } else {
        if(config == null) log.info(s"Initialized") else log.info("Reinitialized")
      }
      config = _config
      dao = new UserProfileCommandDao(config.eagleServiceContext.serviceHost,config.eagleServiceContext.servicePort,config.eagleServiceContext.username,config.eagleServiceContext.password,this)
    case CheckOndemandTrainingStatus(site,category) =>
      val _sender = sender()
      dao.readNewInitializedCommandByType(site,category) onComplete {
        case Success(optionalEntities) =>
          optionalEntities match {
            case Some(entities) =>
              log.info(s"Load ${entities.size} new $category commands")
              JavaConversions.collectionAsScalaIterable(entities) foreach { entity =>
                dao.updateCommandStatus(entity,STATUS.PENDING,"Command is pending to execute") onComplete {
                  case Success(response) =>
                    if(response.isSuccess) {
                      _sender ! entity2Command(site, category, entity, config)
                    }else{
                      log.error(s"Got exception to update status as PENDING for command:$entity, due to service exception: ${response.getException}")
                    }
                  case Failure(ex) =>
                    log.error(ex, s"Got exception to update status as PENDING for command:$entity, due to: ${ex.getMessage}")
                }
              }
            case None =>
              if(log.isDebugEnabled) {
                log.debug(s"Loaded 0 new $category commands")
              }
          }
        case Failure(exception:Exception) => {
          log.error(exception, s"Failed to get commands for site = [$site] and category = [$category], due to ${exception.getMessage}")
        }
    }
  }

  private def entity2Command(site:String,category:COMMAND_TYPE.TYPE,entity: ScheduleCommandEntity,config:SchedulerContext):Command = {
    category match {
      case COMMAND_TYPE.USER_PROFILE_TRAINING =>
        val policy = config.trainingSchedulePolicy
        val status = entity.getTimestamp
        val path = Utils.formatPathWithMilliseconds(config.trainingAuditPath)(status)
        UserProfileModelCommand(entity.getTimestamp,path,site,STATUS.INITIALIZED,COMMAND_SOURCE.ONDEMAND,entity.getTimestamp,entity.getUpdateTime,persistable = true)
      case _ => throw new IllegalArgumentException(s"Unknown type of command type: $category")
    }
  }
}

class PeriodicTrainingProducer extends UntypedActor with ActorLogging{
  @volatile var config:SchedulerContext = null
  @volatile var dao:UserProfileCommandDao=null

  import CommandProducer._

  def checkTrainingProgramStatus(site: String,category:COMMAND_TYPE.TYPE,sender: ActorRef): Unit = {
    checkProgramStatus(site,category,config.trainingSchedulePolicy,config.trainingAuditPath,sender,log) {(status,path) =>
      UserProfileModelCommand(status,path,site,STATUS.INITIALIZED,COMMAND_SOURCE.PERIODIC,System.currentTimeMillis(),System.currentTimeMillis(),persistable = false)
    }
  }

  override def onReceive(message: Any): Unit = message match {
    case _config: SchedulerContext =>
      if (log.isDebugEnabled) {
        log.debug(s"Initialized with config: $config")
      } else {
        if(config == null) log.info(s"Initialized") else log.info("Reinitialized")
      }
      config = _config
      dao = new UserProfileCommandDao(config.eagleServiceContext.serviceHost,config.eagleServiceContext.servicePort,config.eagleServiceContext.username,config.eagleServiceContext.password,this)
    case CheckPeriodicTrainingStatus(site,category) =>
      category match {
        case COMMAND_TYPE.USER_PROFILE_TRAINING =>
          log.debug("Checking training program status")
          checkTrainingProgramStatus(site,category,sender())
        case _ =>
          log.error(s"Unknown type of category: $category")
      }
    case _ => throw new IllegalArgumentException(s"Unknown message $message")
  }
}