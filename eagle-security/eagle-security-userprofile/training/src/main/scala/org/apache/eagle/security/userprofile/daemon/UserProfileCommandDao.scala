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

import java.util
import java.util.concurrent.Callable

import akka.actor.UntypedActor
import akka.dispatch.Futures
import akka.event.Logging
import org.apache.eagle.log.entity.GenericServiceAPIResponseEntity
import org.apache.eagle.security.userprofile.daemon.SchedulerContext.COMMAND_TYPE
import org.apache.eagle.security.userprofile.model.ScheduleCommandEntity
import org.apache.eagle.security.userprofile.model.ScheduleCommandEntity.STATUS
import org.apache.eagle.service.client.impl.EagleServiceClientImpl

/**
 * User Profile Command Dao By Eagle Service
 * @since  9/11/15
 */
class UserProfileCommandDao(host:String,port:Int,username:String,password:String,actor:UntypedActor){
  val client = new EagleServiceClientImpl(host,port,username,password)
  val log = Logging.getLogger(actor)

  /**
   * readNewInitializedCommandByType
   *
   * @param site eagle site name
   * @param commandType command type
   * @return Future
   */
  def readNewInitializedCommandByType(site:String,commandType: COMMAND_TYPE.TYPE) = {
    Futures.future(new Callable[Option[java.util.List[ScheduleCommandEntity]]]{
      override def call(): Option[java.util.List[ScheduleCommandEntity]] = {
        val response:GenericServiceAPIResponseEntity[ScheduleCommandEntity]  =
          new EagleServiceClientImpl(host,port,username,password)
            .silence(true)
            .search(s"${ScheduleCommandEntity.ScheduleTaskService}[@site = ${"\"" + site +"\""} AND @type = ${"\""+commandType.toString+"\""} AND (@status = ${"\""+STATUS.INITIALIZED.name()+"\""} or @status = ${"\"\""} or @status is null)]{*}")
            .startTime(0).endTime(Long.MaxValue).pageSize(Int.MaxValue).send()

        if(!response.isSuccess){
          throw new RuntimeException(s"Got server side exception: ${response.getException}")
        }

        if(response.getObj != null && response.getObj.size() == 0) None else Option(response.getObj)
      }
    },actor.getContext().dispatcher)
  }

  /**
   * updateCommandStatus
   *
   * @param task task entity
   * @param status task status
   * @param message message string
   * @return
   */
  def updateCommandStatus(task:ScheduleCommandEntity,status:STATUS,message:String) ={
    Futures.future(new Callable[GenericServiceAPIResponseEntity[String]] {
      override def call(): GenericServiceAPIResponseEntity[String] = {
        if(log.isDebugEnabled) log.debug(s"Updating status of task[$task] as: $status")
        task.setStatus(status.name())
        task.setDetail(s"[${status.name()}] $message")
        task.setUpdateTime(System.currentTimeMillis())
        val response = client.update(util.Arrays.asList(task),classOf[ScheduleCommandEntity])
        if(response.isSuccess){
          log.info(s"Updated status of command [$task] as: $status")
        } else {
          log.error(s"Failed to update status of command [$task] as: $status, because of exception: ${response.getException}")
          throw new RuntimeException(s"Failed to update status due to exception: ${response.getException}")
        }
        response
      }
    },actor.getContext().dispatcher)
  }
}