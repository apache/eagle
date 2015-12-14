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
package org.apache.eagle.metric.kafka;

import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.base.BaseRichSpout;
import com.typesafe.config.Config;
import org.apache.eagle.dataproc.impl.storm.kafka.KafkaSourcedSpoutProvider;
import org.apache.eagle.dataproc.impl.storm.kafka.KafkaSourcedSpoutScheme;
import org.apache.eagle.datastream.ExecutionEnvironments;
import org.apache.eagle.datastream.storm.StormExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.ZkHosts;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class EagleMetricCollectorMain {

    private static final Logger LOG = LoggerFactory.getLogger(EagleMetricCollectorMain.class);

    public static void main(String[] args) throws Exception {
        StormExecutionEnvironment env = ExecutionEnvironments.getStorm(args);
        Config config = env.getConfig();
        String deserClsName = config.getString("dataSourceConfig.deserializerClass");
        final KafkaSourcedSpoutScheme scheme = new KafkaSourcedSpoutScheme(deserClsName, config) {
            @Override
            public List<Object> deserialize(byte[] ser) {
                Object tmp = deserializer.deserialize(ser);
                Map<String, Object> map = (Map<String, Object>)tmp;
                if(tmp == null) return null;
                return Arrays.asList(map.get("user"), map.get("timestamp"));
            }
        };


        // TODO: Refactored the anonymous in to independen class file, avoiding too complex logic in main method
        KafkaSourcedSpoutProvider kafkaMessageSpoutProvider = new KafkaSourcedSpoutProvider() {
            @Override
            public BaseRichSpout getSpout(Config context) {
                // Kafka topic
                String topic = context.getString("dataSourceConfig.topic");
                // Kafka consumer group id
                String groupId = context.getString("dataSourceConfig.metricCollectionConsumerId");
                // Kafka fetch size
                int fetchSize = context.getInt("dataSourceConfig.fetchSize");
                // Kafka deserializer class
                String deserClsName = context.getString("dataSourceConfig.deserializerClass");

                // Kafka broker zk connection
                String zkConnString = context.getString("dataSourceConfig.zkQuorum");

                // transaction zkRoot
                String zkRoot = context.getString("dataSourceConfig.transactionZKRoot");

                LOG.info(String.format("Use topic id: %s",topic));

                String brokerZkPath = null;
                if(context.hasPath("dataSourceConfig.brokerZkPath")) {
                    brokerZkPath = context.getString("dataSourceConfig.brokerZkPath");
                }

                BrokerHosts hosts;
                if(brokerZkPath == null) {
                    hosts = new ZkHosts(zkConnString);
                } else {
                    hosts = new ZkHosts(zkConnString, brokerZkPath);
                }

                SpoutConfig spoutConfig = new SpoutConfig(hosts,
                        topic,
                        zkRoot + "/" + topic,
                        groupId);

                // transaction zkServers
                String[] zkConnections = zkConnString.split(",");
                List<String> zkHosts = new ArrayList<>();
                for (String zkConnection : zkConnections) {
                    zkHosts.add(zkConnection.split(":")[0]);
                }
                Integer zkPort = Integer.valueOf(zkConnections[0].split(":")[1]);

                spoutConfig.zkServers = zkHosts;
                // transaction zkPort
                spoutConfig.zkPort = zkPort;
                // transaction update interval
                spoutConfig.stateUpdateIntervalMs = context.getLong("dataSourceConfig.transactionStateUpdateMS");
                // Kafka fetch size
                spoutConfig.fetchSizeBytes = fetchSize;

                spoutConfig.scheme = new SchemeAsMultiScheme(scheme);
                KafkaSpout kafkaSpout = new KafkaSpout(spoutConfig);
                return kafkaSpout;
            }
        };

        env.fromSpout(new KafkaOffsetSourceSpoutProvider()).withOutputFields(0).nameAs("kafkaLogLagChecker");
        env.fromSpout(kafkaMessageSpoutProvider).withOutputFields(2).nameAs("kafkaMessageFetcher").groupBy(Arrays.asList(0))
                .flatMap(new KafkaMessageDistributionExecutor());
        env.execute();
    }
}