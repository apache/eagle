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

import backtype.storm.spout.Scheme;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.base.BaseRichSpout;
import com.typesafe.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.ZkHosts;

import java.util.ArrayList;
import java.util.List;

/**
 * Since 8/14/16.
 */
public class KafkaSourcedSpoutProvider {
    private final static Logger LOG = LoggerFactory.getLogger(KafkaSourcedSpoutProvider.class);
    public static BaseRichSpout getSpout(Config context, Scheme scheme) {
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
}
