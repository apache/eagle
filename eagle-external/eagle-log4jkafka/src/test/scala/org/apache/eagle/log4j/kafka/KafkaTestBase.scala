/*
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
package org.apache.eagle.log4j.kafka

import java.util.Properties

import kafka.server.{KafkaConfig, KafkaServerStartable}
import org.apache.curator.framework.{CuratorFramework, CuratorFrameworkFactory}
import org.apache.curator.retry.ExponentialBackoffRetry
import org.apache.curator.test.{InstanceSpec, TestingServer}
import org.junit._
import org.junit.rules.TemporaryFolder
import org.scalatest.junit.JUnitSuite


class KafkaTestBase extends JUnitSuite {
  val _tempFolder = new TemporaryFolder()

  @Rule def tempFolder = _tempFolder

  var zkServer:TestingServer = _
  var curatorClient:CuratorFramework = _
  var kafkaServer:KafkaServerStartable = _
  val kafkaPort = InstanceSpec.getRandomPort
  val zookeeperPort = InstanceSpec.getRandomPort

  @Before
  def before(): Unit = {
    val logDir = tempFolder.newFolder()
    this.zkServer = new TestingServer(zookeeperPort, logDir)
    val retryPolicy = new ExponentialBackoffRetry(1000, 3)
    this.curatorClient = CuratorFrameworkFactory.newClient(zkServer.getConnectString, retryPolicy)
    this.curatorClient.start()

    val p: Properties = new Properties
    p.setProperty("zookeeper.connect", zkServer.getConnectString)
    p.setProperty("broker.id", "0")
    p.setProperty("port", "" + kafkaPort)
    p.setProperty("log.dirs", logDir.getAbsolutePath)
    p.setProperty("auto.create.topics.enable", "true")

    this.kafkaServer = new KafkaServerStartable(new KafkaConfig(p))
    this.kafkaServer.startup()
  }

  @After
  def after(): Unit = {
    kafkaServer.shutdown()
    curatorClient.close()
    zkServer.close()
  }
}
