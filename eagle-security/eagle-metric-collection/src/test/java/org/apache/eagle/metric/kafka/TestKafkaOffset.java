package org.apache.eagle.metric.kafka;
/*
 *
 *    Licensed to the Apache Software Foundation (ASF) under one or more
 *    contributor license agreements.  See the NOTICE file distributed with
 *    this work for additional information regarding copyright ownership.
 *    The ASF licenses this file to You under the Apache License, Version 2.0
 *    (the "License"); you may not use this file except in compliance with
 *    the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 *
 */

import java.util.Map;

import org.apache.eagle.common.config.EagleConfigConstants;
import org.apache.eagle.dataproc.impl.storm.zookeeper.ZKStateConfig;
import org.apache.eagle.service.client.ServiceConfig;
import org.junit.Ignore;
import org.junit.Test;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

public class TestKafkaOffset {

    @Ignore
    @Test
    public void test() throws Exception {
        System.setProperty("config.resource", "/application.local.conf");
        Config config = ConfigFactory.load();
        ZKStateConfig zkStateConfig = new ZKStateConfig();
        zkStateConfig.zkQuorum = config.getString("dataSourceConfig.zkQuorum");
        zkStateConfig.zkRoot = config.getString("dataSourceConfig.transactionZKRoot");
        zkStateConfig.zkSessionTimeoutMs = config.getInt("dataSourceConfig.zkSessionTimeoutMs");
        zkStateConfig.zkRetryTimes = config.getInt("dataSourceConfig.zkRetryTimes");
        zkStateConfig.zkRetryInterval = config.getInt("dataSourceConfig.zkRetryInterval");

        ServiceConfig serviceConfig = new ServiceConfig();
        serviceConfig.serviceHost = config.getString(EagleConfigConstants.EAGLE_PROPS + "." + EagleConfigConstants.EAGLE_SERVICE + "." + EagleConfigConstants.HOST);
        serviceConfig.servicePort = config.getInt(EagleConfigConstants.EAGLE_PROPS + "." + EagleConfigConstants.EAGLE_SERVICE + "." + EagleConfigConstants.PORT);
        serviceConfig.username = config.getString(EagleConfigConstants.EAGLE_PROPS + "." + EagleConfigConstants.EAGLE_SERVICE + "." + EagleConfigConstants.USERNAME);
        serviceConfig.password = config.getString(EagleConfigConstants.EAGLE_PROPS + "." + EagleConfigConstants.EAGLE_SERVICE + "." + EagleConfigConstants.PASSWORD);

        KafkaOffsetCheckerConfig.KafkaConfig kafkaConfig = new KafkaOffsetCheckerConfig.KafkaConfig();
        kafkaConfig.kafkaEndPoints = config.getString("dataSourceConfig.kafkaEndPoints");
        kafkaConfig.site = config.getString("dataSourceConfig.site");
        kafkaConfig.topic = config.getString("dataSourceConfig.topic");
        kafkaConfig.group = config.getString("dataSourceConfig.hdfsTopologyConsumerGroupId");
        KafkaOffsetCheckerConfig checkerConfig = new KafkaOffsetCheckerConfig(serviceConfig, zkStateConfig, kafkaConfig);

        KafkaConsumerOffsetFetcher consumerOffsetFetcher = new KafkaConsumerOffsetFetcher(checkerConfig.zkConfig, checkerConfig.kafkaConfig.topic, checkerConfig.kafkaConfig.group);
        KafkaLatestOffsetFetcher latestOffsetFetcher = new KafkaLatestOffsetFetcher(checkerConfig.kafkaConfig.kafkaEndPoints);

        Map<String, Long> consumedOffset = consumerOffsetFetcher.fetch();
        Map<Integer, Long> latestOffset = latestOffsetFetcher.fetch(checkerConfig.kafkaConfig.topic, consumedOffset.size());
        for (Map.Entry<String, Long> entry : consumedOffset.entrySet()) {
            String partition = entry.getKey();
            Integer partitionNumber = Integer.valueOf(partition.split("_")[1]);
            Long lag = latestOffset.get(partitionNumber) - entry.getValue();
            System.out.println("total: " + latestOffset.get(partitionNumber) + ", consumed: " + entry.getValue() + ",lag: " + lag);
        }
    }
}
