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
package org.apache.eagle.dataproc.impl.storm.kafka;

import java.util.Arrays;

import com.typesafe.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.ZkHosts;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.base.BaseRichSpout;

import org.apache.eagle.dataproc.impl.storm.AbstractStormSpoutProvider;

public class KafkaSourcedSpoutProvider extends AbstractStormSpoutProvider{
    private final static Logger LOG = LoggerFactory.getLogger(KafkaSourcedSpoutProvider.class);

	public SchemeAsMultiScheme getStreamScheme(String deserClsName, Config context) {
		return new SchemeAsMultiScheme(new KafkaSourcedSpoutScheme(deserClsName, context));
	}

	@Override
	public BaseRichSpout getSpout(Config context){
		// Kafka topic
		String topic = context.getString("dataSourceConfig.topic");
		// Kafka consumer group id
		String groupId = context.getString("dataSourceConfig.consumerGroupId");
		// Kafka fetch size
		int fetchSize = context.getInt("dataSourceConfig.fetchSize");
		// Kafka deserializer class
		String deserClsName = context.getString("dataSourceConfig.deserializerClass");
		// Kafka broker zk connection
		String zkConnString = context.getString("dataSourceConfig.zkConnection");
		// transaction zkRoot
		String zkRoot = context.getString("dataSourceConfig.transactionZKRoot");
		// Site
		String site = context.getString("eagleProps.site");

        //String realTopic = (site ==null)? topic : String.format("%s_%s",site,topic);

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
		spoutConfig.zkServers = Arrays.asList(context.getString("dataSourceConfig.transactionZKServers").split(","));
		// transaction zkPort
		spoutConfig.zkPort = context.getInt("dataSourceConfig.transactionZKPort");
		// transaction update interval
		spoutConfig.stateUpdateIntervalMs = context.getLong("dataSourceConfig.transactionStateUpdateMS");
		// Kafka fetch size
		spoutConfig.fetchSizeBytes = fetchSize;		
		// "startOffsetTime" is for test usage, prod should not use this
		if (context.hasPath("dataSourceConfig.startOffsetTime")) {
			spoutConfig.startOffsetTime = context.getInt("dataSourceConfig.startOffsetTime");
		}		
		// "forceFromStart" is for test usage, prod should not use this 
		if (context.hasPath("dataSourceConfig.forceFromStart")) {
			spoutConfig.forceFromStart = context.getBoolean("dataSourceConfig.forceFromStart");
		}
		
		spoutConfig.scheme = getStreamScheme(deserClsName, context);
		KafkaSpout kafkaSpout = new KafkaSpout(spoutConfig);
		return kafkaSpout;
	}
}