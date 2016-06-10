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

package org.apache.eagle.security.topo;

import backtype.storm.spout.Scheme;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.base.BaseRichSpout;
import com.typesafe.config.Config;
import org.apache.eagle.dataproc.impl.storm.StormSpoutProvider;
import org.apache.eagle.dataproc.impl.storm.kafka.KafkaSourcedSpoutScheme;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.ZkHosts;

import java.util.Arrays;

/**
 * Since 6/8/16.
 */
public class NewKafkaSourcedSpoutProvider implements StormSpoutProvider {
    private final static Logger LOG = LoggerFactory.getLogger(NewKafkaSourcedSpoutProvider.class);

    private String configPrefix = "dataSourceConfig";

    public NewKafkaSourcedSpoutProvider(){}

    public NewKafkaSourcedSpoutProvider(String prefix){
        this.configPrefix = prefix;
    }

    @Override
    public BaseRichSpout getSpout(Config config){
        Config context = config;
        if(this.configPrefix!=null) context = config.getConfig(configPrefix);
        // Kafka topic
        String topic = context.getString("topic");
        // Kafka consumer group id
        String groupId = context.getString("consumerGroupId");
        // Kafka fetch size
        int fetchSize = context.getInt("fetchSize");
        // Kafka deserializer class
        String deserClsName = context.getString("deserializerClass");
        // Kafka broker zk connection
        String zkConnString = context.getString("zkConnection");
        // transaction zkRoot
        String zkRoot = context.getString("transactionZKRoot");

        LOG.info(String.format("Use topic id: %s",topic));

        String brokerZkPath = null;
        if(context.hasPath("brokerZkPath")) {
            brokerZkPath = context.getString("brokerZkPath");
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
        spoutConfig.zkServers = Arrays.asList(context.getString("transactionZKServers").split(","));
        // transaction zkPort
        spoutConfig.zkPort = context.getInt("transactionZKPort");
        // transaction update interval
        spoutConfig.stateUpdateIntervalMs = context.getLong("transactionStateUpdateMS");
        // Kafka fetch size
        spoutConfig.fetchSizeBytes = fetchSize;
        // "startOffsetTime" is for test usage, prod should not use this
        if (context.hasPath("startOffsetTime")) {
            spoutConfig.startOffsetTime = context.getInt("startOffsetTime");
        }
        // "forceFromStart" is for test usage, prod should not use this
        if (context.hasPath("forceFromStart")) {
            spoutConfig.forceFromStart = context.getBoolean("forceFromStart");
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
