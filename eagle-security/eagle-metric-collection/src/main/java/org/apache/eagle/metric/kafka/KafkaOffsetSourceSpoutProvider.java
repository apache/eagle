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
package org.apache.eagle.metric.kafka;

import org.apache.storm.topology.base.BaseRichSpout;
import com.typesafe.config.Config;
import org.apache.eagle.common.config.EagleConfigConstants;
import org.apache.eagle.dataproc.impl.storm.StormSpoutProvider;
import org.apache.eagle.dataproc.impl.storm.zookeeper.ZKStateConfig;
import org.apache.eagle.service.client.ServiceConfig;

public class KafkaOffsetSourceSpoutProvider implements StormSpoutProvider {
	public BaseRichSpout getSpout(Config config){

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
		KafkaOffsetSpout spout = new KafkaOffsetSpout(checkerConfig);
		return spout;
	}
}
