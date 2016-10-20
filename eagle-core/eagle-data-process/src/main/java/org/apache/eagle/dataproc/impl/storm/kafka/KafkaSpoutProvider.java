/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.eagle.dataproc.impl.storm.kafka;

import backtype.storm.spout.Scheme;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.base.BaseRichSpout;
import com.typesafe.config.Config;
import org.apache.eagle.dataproc.impl.storm.StormSpoutProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.ZkHosts;

import java.util.Arrays;
import java.util.stream.Collectors;

/**
 * Since 6/8/16.
 */
public class KafkaSpoutProvider implements StormSpoutProvider {
    private final static Logger LOG = LoggerFactory.getLogger(KafkaSpoutProvider.class);
    private final static String DEFAULT_CONFIG_PREFIX = "dataSourceConfig";
    private final static String DEFAULT_CONSUMER_GROUP_ID = "eagleConsumer";
    private final static String DEFAULT_TRANSACTION_ZK_ROOT = "/consumers";

    private String configPrefix = DEFAULT_CONFIG_PREFIX;

    public KafkaSpoutProvider(){}

    public KafkaSpoutProvider(String prefix){
        this.configPrefix = prefix;
    }

    @Override
    public BaseRichSpout getSpout(Config config){
        Config context = config;
        if(this.configPrefix!=null) context = config.getConfig(configPrefix);

        // the following is for fetching data from one topic
        // Kafka topic
        String topic = context.getString("topic");
        // Kafka broker zk connection
        String zkConnString = context.getString("zkConnection");
        // Kafka fetch size
        int fetchSize = context.hasPath("fetchSize") ? context.getInt("fetchSize") : 1048576;
        LOG.info(String.format("Use topic : %s, zkConnection : %s , fetchSize : %d", topic, zkConnString, fetchSize));

        /*
         the following is for recording offset for processing the data
         the zk path to store current offset is comprised of the following
         offset zkPath = zkRoot + "/" + topic + "/" + consumerGroupId + "/" + partition_Id

         consumerGroupId is for differentiating different consumers which consume the same topic
        */
        // transaction zkRoot
        String zkRoot = context.hasPath("transactionZKRoot") ? context.getString("transactionZKRoot") : DEFAULT_TRANSACTION_ZK_ROOT;
        // Kafka consumer group id
        String groupId = context.hasPath("consumerGroupId") ? context.getString("consumerGroupId") : DEFAULT_CONSUMER_GROUP_ID;
        String brokerZkPath = context.hasPath("brokerZkPath") ? context.getString("brokerZkPath") : null;
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
        String[] txZkServers = context.hasPath("txZkServers") ? context.getString("txZkServers").split(",") : new String[]{"localhost:2181"};
        spoutConfig.zkServers = Arrays.asList(txZkServers).stream().map(server -> server.split(":")[0]).collect(Collectors.toList());
        // transaction zkPort
        spoutConfig.zkPort = Integer.parseInt(txZkServers[0].split(":")[1]);
        LOG.info("txZkServers:" + spoutConfig.zkServers + ", zkPort:" + spoutConfig.zkPort);
        // transaction update interval
        spoutConfig.stateUpdateIntervalMs = context.hasPath("transactionStateUpdateMS") ? context.getLong("transactionStateUpdateMS") : 2000;
        // Kafka fetch size
        spoutConfig.fetchSizeBytes = fetchSize;
        spoutConfig.startOffsetTime = kafka.api.OffsetRequest.LatestTime();

        // "startOffsetTime" is for test usage, prod should not use this
        if (context.hasPath("startOffsetTime")) {
            spoutConfig.startOffsetTime = context.getInt("startOffsetTime");
        }
        // "forceFromStart" is for test usage, prod should not use this
        if (context.hasPath("forceFromStart")) {
            spoutConfig.startOffsetTime = kafka.api.OffsetRequest.EarliestTime();
        }

        if (context.hasPath("schemeCls")) {
            try {
                Scheme s = (Scheme)Class.forName(context.getString("schemeCls")).newInstance();
                spoutConfig.scheme = new SchemeAsMultiScheme(s);
            }catch(Exception ex){
                LOG.error("error instantiating scheme object");
                throw new IllegalStateException(ex);
            }
        }else{
            String err = "schemeCls must be present";
            LOG.error(err);
            throw new IllegalStateException(err);
        }
        return new KafkaSpout(spoutConfig);
    }
}
