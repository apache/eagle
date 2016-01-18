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
package org.apache.eagle.stream.dsl.execution.impl

import org.apache.eagle.stream.dsl.entity.{AppCommandEntity, AppDefinitionEntity}
import org.apache.eagle.stream.dsl.execution.{StreamEvaluator, StreamAppManager}
import org.apache.eagle.stream.dsl.StreamBuilder._

class StreamAppManagerImpl extends StreamAppManager{
  override def submit(app: AppDefinitionEntity, cmd: AppCommandEntity): Boolean = {
    val code = app.getDefinition.stripMargin
    var newAppStatus: String = AppDefinitionEntity.STATUS.UNKNOWN
    var newCmdStatus: String = AppCommandEntity.Status.PENDING
    try {
      changeAppStatus(app, AppDefinitionEntity.STATUS.STARTING)
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

  override def stop(app: AppDefinitionEntity, cmd: AppCommandEntity): Boolean = ???

  override def start(app: AppDefinitionEntity, cmd: AppCommandEntity): Boolean = ???
}