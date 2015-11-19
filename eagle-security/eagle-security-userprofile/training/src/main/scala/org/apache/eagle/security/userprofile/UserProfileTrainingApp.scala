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
package org.apache.eagle.security.userprofile

import java.util.Properties

import org.apache.eagle.security.userprofile.model.eigen.UserProfileEigenModeler
import org.apache.eagle.security.userprofile.model.kde.UserProfileKDEModeler
import org.apache.eagle.security.userprofile.sink.{UserActivityAggKafkaSink, UserProfileHDFSSink, UserProfileEagleServiceSink}
import org.apache.eagle.security.userprofile.UserProfileJobFactory._
import org.apache.spark.Logging
import org.joda.time.Period

/**
 * @since  7/28/15
 */
case class UserProfileTrainingApp(config:UserProfileTrainingConfig) extends Logging{
  AuditlogTrainingSparkJob(config.site,config.input, config.master, config.appName, config.cmdTypes, config.period) { job =>
    logInfo(s"Registering UserProfileEigenModeler(${job.cmdTypes.mkString(",")})")
    job.model(new UserProfileEigenModeler(job.cmdTypes.toArray))
    logInfo(s"Registering UserProfileKDEModeler(${job.cmdTypes.mkString(",")})")
    job.model(new UserProfileKDEModeler(job.cmdTypes.toArray))

    if (config.serviceHost != null) {
      logInfo(s"Registering UserProfileEagleServiceSink(${config.serviceHost},${config.servicePort}, ${config.username}, ${config.password})")
      job.sink(new UserProfileEagleServiceSink(config.serviceHost, config.servicePort, config.username, config.password))
    }

    if (config.modelOutput != null) {
      logInfo(s"Registering UserProfileHdfsSink(${config.modelOutput})")
      job.sink(new UserProfileHDFSSink(config.modelOutput))
    }

    if (config.kafkaProps != null) {
      logInfo(s"Registering UserActivityAggKafkaSink(${config.kafkaProps}))")
      job.sink(new UserActivityAggKafkaSink(config.kafkaProps))
    }

    logInfo("Starting to run")

    job.run()
  }
}

/**
 * @param input         audit log input
 * @param period        aggregation period
 * @param master        spark master url
 * @param appName       spark application name
 * @param cmdTypes      command types
 * @param modelOutput   model output path
 * @param serviceHost   service host
 * @param servicePort   service port
 */
case class UserProfileTrainingConfig(site:String = null,
                                     input:String = null,
                                     period:Period = Period.parse("PT1M"), // changing it to 1M aggregation interval
                                     master:String="local[10]",
                                     appName:String="UserProfileTraining",
                                     cmdTypes:Seq[String]=UserProfileConstants.DEFAULT_CMD_TYPES,
                                     modelOutput:String=null,
                                     serviceHost:String=null,
                                     servicePort:Int = 9099,
                                     username:String=null,
                                     password:String=null,
                                     kafkaProps:Properties = null)