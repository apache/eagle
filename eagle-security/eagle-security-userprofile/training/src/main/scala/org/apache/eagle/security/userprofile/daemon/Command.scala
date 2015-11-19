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

import org.apache.eagle.security.userprofile.daemon.SchedulerContext.{COMMAND_SOURCE, COMMAND_TYPE, _}
import org.apache.eagle.security.userprofile.model.ScheduleCommandEntity
import org.apache.eagle.security.userprofile.model.ScheduleCommandEntity.STATUS

import scala.util.{Failure, Success, Try}

/**
 * @since  0.3.0
 */
trait Executable[T]{
  def execute:(SchedulerContext => Try[T])
}

/**
 * @since  0.3.0
 */
trait ShellExecutable extends Executable[String] {
    def shell:(SchedulerContext => Seq[String])
    override def execute = { context =>
      val command = shell(context)
      var result:Try[String]  = null
      var process:Process = null
      try {
        val builder = new ProcessBuilder(scala.collection.JavaConversions.seqAsJavaList(command))
        builder.redirectOutput(ProcessBuilder.Redirect.INHERIT)
        builder.redirectError(ProcessBuilder.Redirect.INHERIT)
        process = builder.start()
        process.waitFor()
        val exitCode = process.exitValue()
        if(exitCode != 0 ) {
          result = Failure(new IllegalMonitorStateException(s"Exit code of process is not zero, but: $exitCode"))
        }else {
          result = Success(s"Successfully executed command $command")
        }
      }catch{
        case e: Exception => result = Failure(e)
      } finally {
        if(process!=null) {
          process.destroy()
        }
      }
      result
    }
}

abstract class Command(protected val timestamp:Long,protected val site:String,protected val category: COMMAND_TYPE.TYPE,source: COMMAND_SOURCE.TYPE,protected val status:ScheduleCommandEntity.STATUS, val updateTime:Long, val persistable:Boolean) {}

abstract class UserProfileCommand(
                               timestamp:Long,
                               inputPath:String,
                               site:String,
                               override val category: COMMAND_TYPE.TYPE,
                               status:STATUS,
                               source: COMMAND_SOURCE.TYPE,
                               updateTime:Long,
                               persistable:Boolean ) extends Command(timestamp,site,category,source,status,updateTime,persistable) with ShellExecutable

case class UserProfileModelCommand(override val timestamp:Long,
                                   inputPath:String,
                                   override val site:String,
                                    override val status:ScheduleCommandEntity.STATUS,
                                   source: COMMAND_SOURCE.TYPE,
                                   createTime:Long,
                                   override val updateTime:Long,
                                   override val persistable:Boolean = true ) extends UserProfileCommand(timestamp,inputPath,site,COMMAND_TYPE.USER_PROFILE_TRAINING,status,source,updateTime,persistable) {

  override def shell = { context: SchedulerContext =>
    var builder = Seq(context.driverShell)
    if(context.driverClasspath!=null)
      builder = builder ++ Seq("--driver-class-path",context.driverClasspath)
    builder ++ Seq (
      "--master",context.sparkMaster,
      "--deploy-mode",context.sparkMode,
      "--class",UserProfileTrainingCLIClass,context.jobJar,
      "--site",this.site,
      "--period",context.period,
      "--input", this.inputPath,
      "--service-host",context.eagleServiceContext.serviceHost,
      "--service-port",context.eagleServiceContext.servicePort.toString,
      "--service-username", context.eagleServiceContext.username,
      "--service-password", context.eagleServiceContext.password
    )
  }
}

object Command{
  def asEntity(command:Command):ScheduleCommandEntity = {
    val entity = new ScheduleCommandEntity
    val tags = new java.util.HashMap[String,String]
    tags.put("site",command.site)
    tags.put("type",command.category.toString)
    entity.setTags(tags)
    entity.setTimestamp(command.timestamp)
    entity.setUpdateTime(command.updateTime)
    entity.setStatus(command.status.toString)
    entity
  }
}