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

import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.utils.Utils;
import com.typesafe.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.storm.kafka.bolt.KafkaBolt;

import java.util.HashMap;
import java.util.Map;

/**
 * Since 6/8/16.
 */
public class TopologySubmitter {
    public final static String LOCAL_MODE = "topology.localMode";
    public final static String MESSAGE_TIMEOUT_SECS = "topology.messageTimeoutSecs";
    public final static String TOTAL_WORKER_NUM = "topology.numOfTotalWorkers";
    public final static int DEFAULT_MESSAGE_TIMEOUT_SECS = 3600;

    private static Logger LOG = LoggerFactory.getLogger(TopologySubmitter.class);

    public static void submit(StormTopology topology, Config config){
        org.apache.storm.Config stormConfig = new org.apache.storm.Config();
        int messageTimeoutSecs = config.hasPath(MESSAGE_TIMEOUT_SECS)?config.getInt(MESSAGE_TIMEOUT_SECS) : DEFAULT_MESSAGE_TIMEOUT_SECS;
        LOG.info("Set topology.message.timeout.secs as {}",messageTimeoutSecs);
        stormConfig.setMessageTimeoutSecs(messageTimeoutSecs);

        // set kafka sink
        if(config.hasPath("dataSinkConfig.brokerList")){
            Map props = new HashMap<>();
            props.put("metadata.broker.list", config.getString("dataSinkConfig.brokerList"));
            props.put("serializer.class", config.getString("dataSinkConfig.serializerClass"));
            props.put("key.serializer.class", config.getString("dataSinkConfig.keySerializerClass"));
            //  KafkaBolt.KAFKA_BROKER_PROPERTIES is removed, using direct string
            stormConfig.put("kafka.broker.properties", props);
        }

        if(config.hasPath("dataSinkConfig.serializerClass")){
            stormConfig.put(KafkaBolt.TOPIC, config.getString("dataSinkConfig.topic"));
        }

        if(config.hasPath("dataSinkConfig.topic")){
            stormConfig.put(KafkaBolt.TOPIC, config.getString("dataSinkConfig.topic"));
        }

        boolean localMode = config.getBoolean(LOCAL_MODE);
        int numOfTotalWorkers = config.getInt(TOTAL_WORKER_NUM);
        stormConfig.setNumWorkers(numOfTotalWorkers);
        String topologyId = config.getString("topology.name");
        if(localMode) {
            LOG.info("Submitting as local mode");
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology(topologyId, stormConfig, topology);
            Utils.sleep(Long.MAX_VALUE);
        }else{
            LOG.info("Submitting as cluster mode");
            try {
                StormSubmitter.submitTopologyWithProgressBar(topologyId, stormConfig, topology);
            } catch(Exception ex) {
                LOG.error("fail submitting topology {}", topology, ex);
                throw new IllegalStateException(ex);
            }
        }
    }
}
