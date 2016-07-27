/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

package org.apache.eagle.stream.application

import org.apache.eagle.service.application.AppManagerConstants
import org.apache.eagle.stream.application.impl.StormExecutionPlatform
import org.slf4j.{LoggerFactory, Logger}

import scala.collection.mutable


object ExecutionPlatformFactory {
  private val LOG: Logger = LoggerFactory.getLogger(ExecutionPlatformFactory.getClass)

  var managerCache = new mutable.HashMap[String, ExecutionPlatform] with
    mutable.SynchronizedMap[String, ExecutionPlatform]

  def getApplicationManager(managerType: String): ExecutionPlatform = {
    if(managerCache.contains(managerType)) {
      managerCache.get(managerType).get
    } else {
      managerType match {
        case AppManagerConstants.EAGLE_CLUSTER_STORM =>
          val instance = new StormExecutionPlatform
          managerCache.put(managerType, instance)
          instance
        case _ =>
          throw new Exception(s"Invalid managerType $managerType")
      }
    }
  }

}
