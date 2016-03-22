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

package org.apache.eagle.stream.application.impl

import java.net.URLDecoder
import java.nio.file.{Files, Paths}

import com.typesafe.config.Config
import org.apache.eagle.datastream.ExecutionEnvironments
import org.apache.eagle.datastream.storm.StormExecutionEnvironment
import org.apache.eagle.stream.application.{ApplicationManager, AbstractDynamicApplication}
import org.slf4j.LoggerFactory


object StormDynamicTopology extends AbstractDynamicApplication {
  val LOG = LoggerFactory.getLogger(classOf[AbstractDynamicApplication])

  override def submit(application: String, config: Config) {
    val stream = compileStream(application, config)
    var ret = true

    try {
      val stormJarPath: String = URLDecoder.decode(classOf[ApplicationManager].getProtectionDomain.getCodeSource.getLocation.getPath, "UTF-8")
      if (stormJarPath == null || !Files.exists(Paths.get(stormJarPath)) || !stormJarPath.endsWith(".jar")) {
        val errMsg = s"storm jar file $stormJarPath does not exists, or is a invalid jar file"
        LOG.error(errMsg)
        throw new Exception(errMsg)
      }
      System.setProperty("storm.jar", stormJarPath)
      val stormEnv = ExecutionEnvironments.getWithConfig[StormExecutionEnvironment](stream.getConfig)
      stream.submit(stormEnv)
    } catch {
      case e: Throwable =>
        ret = false
        LOG.error(e.toString)
    }
  }
}
