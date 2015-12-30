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
package org.apache.eagle.stream.dsl.execution

import backtype.storm.StormSubmitter.ProgressListener
import backtype.storm.topology.TopologyBuilder
import backtype.storm.utils.NimbusClient
import backtype.storm.{Config, StormSubmitter}
import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.eagle.stream.dsl.StreamApp
import org.slf4j.LoggerFactory

/**
 * Should build as jar firstly then run with `java -cp package.jar org.apache.eagle.stream.dsl.execution.StormSubmitterSpec`
 */
object StormSubmitterSpec extends App{
  val runningJar = classOf[StreamApp].getProtectionDomain.getCodeSource.getLocation.getPath // should build as jar firstly
  val logger = LoggerFactory.getLogger(classOf[App])
  val conf: Config = new Config()
  conf.put(Config.NIMBUS_HOST, "localhost")
  conf.put(Config.NIMBUS_THRIFT_PORT, Int.box(6627))
  conf.put(Config.STORM_THRIFT_TRANSPORT_PLUGIN, "backtype.storm.security.auth.SimpleTransportPlugin")

  val location = StormSubmitter.submitJar(conf,runningJar,new ProgressListener {
    override def onStart(srcFile: String, targetFile: String, totalBytes: Long): Unit = {
      logger.info(s"Submitting $srcFile to $targetFile (totally $totalBytes bytes)")
    }

    override def onProgress(srcFile: String, targetFile: String, bytesUploaded: Long, totalBytes: Long): Unit = {
//      logger.info(s"$srcFile ==> $targetFile $bytesUploaded/$totalBytes bytes)")
    }

    override def onCompleted (srcFile: String, targetFile: String, totalBytes: Long):Unit = {
      logger.info(s"Successfully completed ")
    }
  })

  val builder = new TopologyBuilder()
  NimbusClient.getConfiguredClient(conf).getClient.submitTopology("sampleTopology",location,new ObjectMapper().writeValueAsString(conf),builder.createTopology())
}