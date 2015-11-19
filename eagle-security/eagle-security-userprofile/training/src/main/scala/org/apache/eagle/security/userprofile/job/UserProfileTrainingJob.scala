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
package org.apache.eagle.security.userprofile.job

import org.apache.eagle.security.userprofile.model.{UserProfileModel, UserProfileContext, UserProfileModeler}
import org.apache.eagle.security.userprofile.sink.{UserActivityAggRDDSink, UserProfileModelRDDSink}

import scala.collection.mutable

/**
 * User Profile Training Job Interface
 *
 * @since  7/19/15
 */
trait UserProfileTrainingJob {
  /**
   * Start the app
   *
   * @return
   */
  def run()

  /**
   * Register training modeler
   *
   * @param modeler
   */
  def model(modeler:UserProfileModeler[UserProfileModel,UserProfileContext]): UserProfileTrainingJob = {
    this.modelers += modeler
    this
  }

  /**
   * Register model data consumer
   * @param sink
   */
  def sink(sink: UserProfileModelRDDSink): UserProfileTrainingJob = {
    this.modelSinks += sink
    this
  }

  def sink(sink: UserActivityAggRDDSink): UserProfileTrainingJob = {
    this.aggSinks += sink
    this
  }

  val modelers = new mutable.MutableList[UserProfileModeler[UserProfileModel,UserProfileContext]]()
  val modelSinks = new mutable.MutableList[UserProfileModelRDDSink]()
  val aggSinks = new mutable.MutableList[UserActivityAggRDDSink]()
}
