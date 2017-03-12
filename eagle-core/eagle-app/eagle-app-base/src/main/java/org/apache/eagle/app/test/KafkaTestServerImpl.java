/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.eagle.app.test;

import kafka.server.KafkaConfig;
import kafka.server.KafkaServerStartable;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.InstanceSpec;
import org.apache.curator.test.TestingServer;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

class KafkaTestServerImpl implements KafkaTestServer {

    private final File logDir;
    private TestingServer zkServer;
    private CuratorFramework curatorClient;
    private KafkaServerStartable kafkaServer;
    private int kafkaPort = InstanceSpec.getRandomPort();
    private int zookeeperPort = InstanceSpec.getRandomPort();

    public KafkaTestServerImpl(File logDir) {
        this.logDir = logDir;
    }

    @Override
    public void start() throws Exception {
        zkServer = new TestingServer(zookeeperPort, logDir);
        ExponentialBackoffRetry retryPolicy = new ExponentialBackoffRetry(1000, 3);
        curatorClient = CuratorFrameworkFactory.newClient(zkServer.getConnectString(), retryPolicy);
        curatorClient.start();

        Properties props = new Properties();

        props.setProperty("zookeeper.connect", zkServer.getConnectString());
        props.setProperty("broker.id", "0");
        props.setProperty("port", "" + kafkaPort);
        props.setProperty("log.dirs", logDir.getAbsolutePath());
        props.setProperty("auto.create.topics.enable", "true");

        kafkaServer = new KafkaServerStartable(new KafkaConfig(props));
        kafkaServer.startup();
    }

    @Override
    public void stop() throws IOException {
        kafkaServer.shutdown();
        curatorClient.close();
        zkServer.close();
    }

    @Override
    public int getZookeeperPort() {
        return this.zookeeperPort;
    }

    @Override
    public int getKafkaBrokerPort() {
        return this.kafkaPort;
    }
}
