/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one or more
 *  * contributor license agreements.  See the NOTICE file distributed with
 *  * this work for additional information regarding copyright ownership.
 *  * The ASF licenses this file to You under the Apache License, Version 2.0
 *  * (the "License"); you may not use this file except in compliance with
 *  * the License.  You may obtain a copy of the License at
 *  *
 *  *    http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */
package org.apache.storm.kafka;

import org.apache.eagle.alert.coordination.model.SpoutSpec;
import org.apache.eagle.alert.engine.coordinator.StreamDefinition;
import org.apache.eagle.alert.engine.spout.ISpoutSpecLCM;
import org.apache.eagle.alert.engine.spout.SpoutOutputCollectorWrapper;
import org.apache.storm.Config;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * NOTE!!!!! This class copy/paste some code from storm.kafka.KafkaSpout to make sure it can support one process to hold multiple
 * KafkaSpout
 *
 * <p>this collectorWrapper provides the following capabilities:
 * 1. inject customized collector collectorWrapper, so framework can control traffic routing
 * 2. listen to topic to stream metadata change and pass that to customized collector collectorWrapper
 * 3. return current streams for this topic</p>
 */
public class KafkaSpoutWrapper extends KafkaSpout implements ISpoutSpecLCM {
    private static final long serialVersionUID = 5507693757424351306L;
    @SuppressWarnings("unused")
    private static final Logger LOG = LoggerFactory.getLogger(KafkaSpoutWrapper.class);
    private KafkaSpoutMetric kafkaSpoutMetric;

    public KafkaSpoutWrapper(SpoutConfig spoutConf, KafkaSpoutMetric kafkaSpoutMetric) {
        super(spoutConf);
        this.kafkaSpoutMetric = kafkaSpoutMetric;
    }

    private SpoutOutputCollectorWrapper collectorWrapper;

    @SuppressWarnings( {"unchecked", "rawtypes"})
    @Override
    public void open(Map conf, final TopologyContext context, final SpoutOutputCollector collector) {
        String topologyInstanceId = context.getStormId();
        ////// !!!! begin copying code from storm.kafka.KafkaSpout to here
        _collector = collector;

        Map stateConf = new HashMap(conf);
        List<String> zkServers = _spoutConfig.zkServers;
        if (zkServers == null) {
            zkServers = (List<String>) conf.get(Config.STORM_ZOOKEEPER_SERVERS);
        }
        Integer zkPort = _spoutConfig.zkPort;
        if (zkPort == null) {
            zkPort = ((Number) conf.get(Config.STORM_ZOOKEEPER_PORT)).intValue();
        }
        stateConf.put(Config.TRANSACTIONAL_ZOOKEEPER_SERVERS, zkServers);
        stateConf.put(Config.TRANSACTIONAL_ZOOKEEPER_PORT, zkPort);
        stateConf.put(Config.TRANSACTIONAL_ZOOKEEPER_ROOT, _spoutConfig.zkRoot);
        _state = new ZkState(stateConf);

        _connections = new DynamicPartitionConnections(_spoutConfig, KafkaUtils.makeBrokerReader(conf, _spoutConfig));

        // using TransactionalState like this is a hack
        int totalTasks = context.getComponentTasks(context.getThisComponentId()).size();
        if (_spoutConfig.hosts instanceof StaticHosts) {
            _coordinator = new StaticCoordinator(_connections, conf, _spoutConfig, _state, context.getThisTaskIndex(), totalTasks, topologyInstanceId);
        } else {
            _coordinator = new ZkCoordinator(_connections, conf, _spoutConfig, _state, context.getThisTaskIndex(), totalTasks, topologyInstanceId);
        }

        ////// !!!! end copying code from storm.kafka.KafkaSpout to here

        // add new topic to metric
        KafkaSpoutMetric.KafkaSpoutMetricContext metricContext = new KafkaSpoutMetric.KafkaSpoutMetricContext();
        metricContext.connections = _connections;
        metricContext.coordinator = _coordinator;
        metricContext.spoutConfig = _spoutConfig;
        kafkaSpoutMetric.addTopic(_spoutConfig.topic, metricContext);

        this.collectorWrapper = (SpoutOutputCollectorWrapper) collector;
    }

    @Override
    public void update(SpoutSpec metadata, Map<String, StreamDefinition> sds) {
        collectorWrapper.update(metadata, sds);
    }

    @Override
    public void close() {
        super.close();
        kafkaSpoutMetric.removeTopic(_spoutConfig.topic);
    }
}