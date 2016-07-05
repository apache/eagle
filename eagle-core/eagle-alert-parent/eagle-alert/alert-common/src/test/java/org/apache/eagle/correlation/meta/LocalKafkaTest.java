package org.apache.eagle.correlation.meta;

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
import java.util.Properties;

import kafka.admin.AdminUtils;
import kafka.admin.RackAwareMode;
import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;

import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;
import org.apache.eagle.alert.utils.KafkaEmbedded;

/**
 * @since Jun 3, 2016
 *
 */
public class LocalKafkaTest {

    /**
     * @param args
     */
    public static void main(String[] args) {
        KafkaEmbedded kafka = new KafkaEmbedded(9092, 2181);

        makeSureTopic("local_kafka_topic");

        while (true) {
            try {
                Thread.sleep(3000);
            } catch (Exception e) {
                break;
            }
        }

        kafka.shutdown();

    }

    public static void makeSureTopic(String topic) {
        ZkClient zkClient = new ZkClient("localhost:2181", 10000, 10000, ZKStringSerializer$.MODULE$);
        Properties topicConfiguration = new Properties();
        ZkConnection zkConnection = new ZkConnection("localhost:2181");
        ZkUtils zkUtils = new ZkUtils(zkClient, zkConnection, false);
        AdminUtils.createTopic(zkUtils, topic, 1, 1, topicConfiguration, RackAwareMode.Disabled$.MODULE$);
    }

}
