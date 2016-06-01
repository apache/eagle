package org.apache.eagle.alert.tools;
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

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.eagle.alert.config.ZKConfig;
import org.junit.Ignore;
import org.junit.Test;

import java.util.Map;

public class TestKafkaOffset {
    @Ignore
    @Test
    public void test() throws Exception {
        System.setProperty("config.resource", "/kafka-offset-test.application.conf");
        Config config = ConfigFactory.load();
        ZKConfig zkConfig = new ZKConfig();
        zkConfig.zkQuorum = config.getString("dataSourceConfig.zkQuorum");
        zkConfig.zkRoot = config.getString("dataSourceConfig.transactionZKRoot");
        zkConfig.zkSessionTimeoutMs = config.getInt("dataSourceConfig.zkSessionTimeoutMs");
        zkConfig.connectionTimeoutMs = config.getInt("dataSourceConfig.connectionTimeoutMs");
        zkConfig.zkRetryTimes = config.getInt("dataSourceConfig.zkRetryTimes");
        zkConfig.zkRetryInterval = config.getInt("dataSourceConfig.zkRetryInterval");

        String topic = "testTopic1";
        String topology = "alertUnitTopology_1";

        while(true) {
            KafkaConsumerOffsetFetcher consumerOffsetFetcher = new KafkaConsumerOffsetFetcher(zkConfig, topic, topology);
            String kafkaBrokerList = config.getString("dataSourceConfig.kafkaBrokerList");
            KafkaLatestOffsetFetcher latestOffsetFetcher = new KafkaLatestOffsetFetcher(kafkaBrokerList);

            Map<String, Long> consumedOffset = consumerOffsetFetcher.fetch();
            if(consumedOffset.size() == 0){
                System.out.println("no any consumer offset found for this topic " + topic);
            }
            Map<Integer, Long> latestOffset = latestOffsetFetcher.fetch(topic, consumedOffset.size());
            if(latestOffset.size() == 0){
                System.out.println("no any latest offset found for this topic " + topic);
            }
            for (Map.Entry<String, Long> entry : consumedOffset.entrySet()) {
                String partition = entry.getKey();
                Integer partitionNumber = Integer.valueOf(partition.split("_")[1]);
                Long lag = latestOffset.get(partitionNumber) - entry.getValue();
                System.out.println(String.format("parition %s, total: %d, consumed: %d, lag: %d",
                        partition, latestOffset.get(partitionNumber), entry.getValue(), lag));
            }
            Thread.sleep(10000);
        }
    }
}
