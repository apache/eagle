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

import java.net.URLDecoder

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.eagle.security.userprofile.UserProfileTrainingCLI

object SchedulerContext {
  val SchedulerSystem = "userprofile-scheduler-system"
  val UserProfileTrainingJarFilePath = URLDecoder.decode(classOf[UserProfileTrainingCLI].getProtectionDomain.getCodeSource.getLocation.getPath,"UTF-8")
  val UserProfileTrainingCLIClass = classOf[UserProfileTrainingCLI].getCanonicalName
  val HdfsAuditLogAggStatus = "hdfs.audit.agg.status"
  val HdfsAuditLogModelStatus = "hdfs.audit.model.status"

  // {
  // ======================================================================================
  // Configuration Keys
  // ======================================================================================
  val UserProfileConfigKey_Site="eagle.site"
  val UserProfileConfigKey_DryRun="eagle.userprofile.dry-run"


  val UserProfileConfigKey_Features="eagle.userprofile.features"
  val UserProfileConfigKey_Period="eagle.userprofile.period"

  val UserProfileConfigKey_JobJarFilePath="eagle.userprofile.jar"
  val UserProfileConfigKey_DriverShell="eagle.userprofile.driver-shell"
  val UserProfileConfigKey_DriverClasspath="eagle.userprofile.driver-classpath"
  val UserProfileConfigKey_SparkMaster="eagle.userprofile.spark-master"
  val UserProfileConfigKey_SparkMode="eagle.userprofile.spark-mode"

  val UserProfileConfigKey_ModelAuditPath="eagle.userprofile.training-audit-path"
  val UserProfileConfigKey_ModelIntervalSeconds="eagle.userprofile.training-interval-seconds"
  val UserProfileConfigKey_ModelInitialDelaySeconds="eagle.userprofile.training-initial-delay-seconds"
  val UserProfileConfigKey_ServiceHost="eagle.service.host"
  val UserProfileConfigKey_ServicePort="eagle.service.port"
  val UserProfileConfigKey_UserName="eagle.service.username"
  val UserProfileConfigKey_Password="eagle.service.password"
  val UserProfileConfigKey_SyncIntervalSeconds="eagle.userprofile.sync-interval-seconds"
  // ======================================================================================
  // End of Configuration Keys
  // ======================================================================================
  // }

  object COMMAND_TYPE extends Enumeration {
    type TYPE = Value
    val USER_PROFILE_TRAINING = Value
  }

  object COMMAND_SOURCE extends Enumeration {
    type TYPE = Value
    val PERIODIC, ONDEMAND  = Value
  }

  /**
   * Load scheduler context from configuration
   */
  def load:SchedulerContext={
    val config:Config = ConfigFactory.load()
    var site = ""

    if(config.hasPath(UserProfileConfigKey_Site)) site = config.getString(UserProfileConfigKey_Site)

    if(site == null || site.isEmpty) throw new IllegalArgumentException(s"Config [$UserProfileConfigKey_Site] is required, but not given")

    val context = new SchedulerContext(site)

    if(config.hasPath(UserProfileConfigKey_DryRun)) context.dryRun = config.getBoolean(UserProfileConfigKey_DryRun)
    if(config.hasPath(UserProfileConfigKey_Features)) context.features = config.getString(UserProfileConfigKey_Features)
    if(config.hasPath(UserProfileConfigKey_Period)) context.period = config.getString(UserProfileConfigKey_Period)

    if(config.hasPath(UserProfileConfigKey_ModelAuditPath)) {
      context.trainingAuditPath = config.getString(UserProfileConfigKey_ModelAuditPath)
    }else{
      throw new IllegalArgumentException(s"Config[$UserProfileConfigKey_ModelAuditPath] should not be null")
    }

    if(config.hasPath(UserProfileConfigKey_ModelIntervalSeconds)) context.trainingIntervalSeconds = config.getLong(UserProfileConfigKey_ModelIntervalSeconds)
    if(config.hasPath(UserProfileConfigKey_ModelInitialDelaySeconds)) context.trainingInitialDelaySeconds = config.getLong(UserProfileConfigKey_ModelInitialDelaySeconds)

    if(config.hasPath(UserProfileConfigKey_ServiceHost)) context.eagleServiceContext.serviceHost = config.getString(UserProfileConfigKey_ServiceHost)
    if(config.hasPath(UserProfileConfigKey_ServicePort)) context.eagleServiceContext.servicePort = config.getInt(UserProfileConfigKey_ServicePort)
    if(config.hasPath(UserProfileConfigKey_UserName)) context.eagleServiceContext.username = config.getString(UserProfileConfigKey_UserName)
    if(config.hasPath(UserProfileConfigKey_Password)) context.eagleServiceContext.password = config.getString(UserProfileConfigKey_Password)

    if(config.hasPath(UserProfileConfigKey_JobJarFilePath)) context.jobJar = config.getString(UserProfileConfigKey_JobJarFilePath)

    if(config.hasPath(UserProfileConfigKey_SparkMaster)) context.sparkMaster = config.getString(UserProfileConfigKey_SparkMaster)
    if(config.hasPath(UserProfileConfigKey_SparkMode)) context.sparkMode = config.getString(UserProfileConfigKey_SparkMode)
    if(config.hasPath(UserProfileConfigKey_DriverShell)) context.driverShell = config.getString(UserProfileConfigKey_DriverShell)
    if(config.hasPath(UserProfileConfigKey_DriverClasspath)) context.driverClasspath = config.getString(UserProfileConfigKey_DriverClasspath)
    if(config.hasPath(UserProfileConfigKey_SyncIntervalSeconds)) context.syncIntervalSeconds = config.getLong(UserProfileConfigKey_SyncIntervalSeconds)

    context
  }
}

case class EagleServiceContext(
  var serviceHost:String = "localhost",
  var servicePort:Int = 9099,
  var username:String = "admin",
  var password:String = "secure"
)

case class SchedulerContext(
  var site:String,
  var dryRun:Boolean = true,
  var features:String = null,
  var period:String = "PT1m", // changing to 1m aggregation
  var eagleServiceContext: EagleServiceContext=new EagleServiceContext(),
  var jobJar: String = SchedulerContext.UserProfileTrainingJarFilePath,
  var driverShell: String = "spark-submit",
  var driverClasspath: String = null,
  var sparkMaster:String = "local[10]",
  var sparkMode:String = "client",
  var trainingAuditPath:String = null,
  var trainingIntervalSeconds:Long = 60,
  var trainingInitialDelaySeconds:Long = 0,
  /**
   * Detection sync commands interval in seconds
   */
  var syncIntervalSeconds:Long = 5,
  /**
   * Training program schedule policy
   */
  trainingSchedulePolicy:SchedulePolicy = new MonthlySchedulePolicy()
)