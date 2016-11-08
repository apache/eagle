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
package org.apache.eagle.alert.utils;

import kafka.server.KafkaConfig;
import kafka.server.KafkaServerStartable;
import org.apache.commons.io.FileUtils;
import org.apache.curator.test.InstanceSpec;

import java.io.File;
import java.util.Properties;

public class KafkaEmbedded {

    private int port;
    private KafkaServerStartable kafka;
    private ZookeeperEmbedded zk;

    private File logDir;

    public KafkaEmbedded() {
        this(InstanceSpec.getRandomPort(), InstanceSpec.getRandomPort());
    }

    public KafkaEmbedded(Integer kafkaPort, Integer zookeeperPort) {
        try {
            zk = new ZookeeperEmbedded(zookeeperPort);
            zk.start();

            this.port = null != kafkaPort ? kafkaPort : InstanceSpec.getRandomPort();
            logDir = new File(System.getProperty("java.io.tmpdir"), "kafka/logs/kafka-test-" + kafkaPort);
            FileUtils.deleteQuietly(logDir);

            KafkaConfig config = buildKafkaConfig(zk.getConnectionString());
            kafka = new KafkaServerStartable(config);
            kafka.startup();
        } catch (Exception ex) {
            throw new RuntimeException("Could not start test broker", ex);
        }
    }

    public KafkaEmbedded(String kafkaUrl, String zkUrl) {
        this(extractKafkaPort(kafkaUrl), extractKafkaPort(zkUrl));

    }

    public static Integer extractKafkaPort(String url) {
        String portString = url.substring(url.indexOf(":") + 1, url.length());
        return Integer.valueOf(portString);
    }

    private KafkaConfig buildKafkaConfig(String zookeeperConnectionString) {
        Properties p = new Properties();
        p.setProperty("zookeeper.connect", zookeeperConnectionString);
        p.setProperty("broker.id", "0");
        p.setProperty("port", "" + port);
        p.setProperty("log.dirs", logDir.getAbsolutePath());
        return new KafkaConfig(p);
    }

    public String getZkConnectionString() {
        return zk.getConnectionString();
    }

    public String getBrokerConnectionString() {
        return "localhost:" + port;
    }

    public int getPort() {
        return port;
    }

    public void shutdown() {
        try {
            kafka.shutdown();
        } catch (Throwable t) {
            t.printStackTrace();
        } finally {
            FileUtils.deleteQuietly(logDir);
        }
        zk.shutdown();
    }

}
