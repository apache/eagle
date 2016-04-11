package org.apache.eagle.embed.kafka;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServerStartable;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;

/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
public class EmbedKafkaServerImpl implements EmbedKafkaServer {
    public KafkaServerStartable kafka;
    public EmbedZookeeperServer zookeeper;
    public EmbedKafkaServerImpl(Properties kafkaProperties, Properties zkProperties) throws IOException {
        zookeeper = new EmbedZookeeperServerImpl(zkProperties);
        kafka = new KafkaServerStartable(new KafkaConfig(kafkaProperties));
    }

    public EmbedKafkaServerImpl(Config config) throws IOException {
        zookeeper = new EmbedZookeeperServerImpl(configToProperties(config.getConfig("zookeeper.server")));
        kafka = new KafkaServerStartable(new KafkaConfig(configToProperties(config.getConfig("kafka.server"))));
    }

    private static Properties configToProperties(Config config){
        Properties properties = new Properties();
        for(Map.Entry<String,Object> entry: config.root().unwrapped().entrySet()){
            properties.put(entry.getKey(),String.valueOf(entry.getValue()));
        }
        return properties;
    }

    public EmbedKafkaServerImpl() throws IOException {
        this(ConfigFactory.load());
    }

    @Override
    public void start() throws IOException {
        //start local zookeeper
        zookeeper.start();
        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        //start local kafka broker
        kafka.startup();
    }

    @Override
    public void stop() {
        kafka.shutdown();
        zookeeper.stop();
    }
}