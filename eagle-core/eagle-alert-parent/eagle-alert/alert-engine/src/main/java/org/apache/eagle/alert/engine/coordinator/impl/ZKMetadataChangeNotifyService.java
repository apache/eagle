/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.eagle.alert.engine.coordinator.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.eagle.alert.config.ConfigBusConsumer;
import org.apache.eagle.alert.config.ConfigChangeCallback;
import org.apache.eagle.alert.config.ConfigValue;
import org.apache.eagle.alert.config.ZKConfig;
import org.apache.eagle.alert.coordination.model.AlertBoltSpec;
import org.apache.eagle.alert.coordination.model.PublishSpec;
import org.apache.eagle.alert.coordination.model.RouterSpec;
import org.apache.eagle.alert.coordination.model.ScheduleState;
import org.apache.eagle.alert.coordination.model.SpoutSpec;
import org.apache.eagle.alert.coordination.model.VersionedPolicyDefinition;
import org.apache.eagle.alert.coordination.model.VersionedStreamDefinition;
import org.apache.eagle.alert.engine.coordinator.MetadataType;
import org.apache.eagle.alert.engine.coordinator.PolicyDefinition;
import org.apache.eagle.alert.engine.coordinator.StreamDefinition;
import org.apache.eagle.alert.service.IMetadataServiceClient;
import org.apache.eagle.alert.service.MetadataServiceClientImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.typesafe.config.Config;

/**
 * <b>TODO</b>: performance tuning: It is not JVM level service, so it may cause
 * zookeeper burden in case of too many listeners This does not support
 * dynamically adding topic, all topics should be available when service object
 * is created.
 * <p>
 * ZK path format is as following:
 * <ul>
 * <li>/alert/topology_1/spout</li>
 * <li>/alert/topology_1/router</li>
 * <li>/alert/topology_1/alert</li>
 * <li>/alert/topology_1/publisher</li>
 * </ul>
 */
public class ZKMetadataChangeNotifyService extends AbstractMetadataChangeNotifyService implements ConfigChangeCallback {
    private static final long serialVersionUID = -1509237694501235144L;
    private static final Logger LOG = LoggerFactory.getLogger(ZKMetadataChangeNotifyService.class);
    private ZKConfig zkConfig;
    private String topologyId;
    private ConfigBusConsumer consumer;

    private transient IMetadataServiceClient client;

    public ZKMetadataChangeNotifyService(ZKConfig config, String topologyId) {
        this.zkConfig = config;
        this.topologyId = topologyId;
    }

    @Override
    public void init(Config config, MetadataType type) {
        super.init(config, type);
        client = new MetadataServiceClientImpl(config);
        consumer = new ConfigBusConsumer(zkConfig, topologyId + "/" + getMetadataTopicSuffix(), this);
        LOG.info("init called for client");
    }

    /**
     * @seeAlso Coordinator
     * @return
     */
    private String getMetadataTopicSuffix() {
        switch (type) {
        case ALERT_BOLT:
            return "alert";
        case ALERT_PUBLISH_BOLT:
            return "publisher";
        case SPOUT:
            return "spout";
        case STREAM_ROUTER_BOLT:
            return "router";
        default:
            throw new RuntimeException(String.format("unexpected metadata type: %s !", type));
        }
    }

    @Override
    public void close() throws IOException {
        consumer.close();
        LOG.info("Config consumer closed");
    }

    @Override
    public void onNewConfig(ConfigValue value) {
        LOG.info("Metadata changed {}",value);

        if (client == null) {
            LOG.error("OnNewConfig trigger, but metadata service client is null. Metadata type {}", type);
            return;
        }

        // analyze config value and notify corresponding listeners
        String version = value.getValue().toString();
        // brute-force load all: this might introduce load's on metadata service.
        // FIXME : after ScheduleState persisted with better normalization, load
        // state based on type and version
        ScheduleState state = client.getVersionedSpec(version);
        if (state == null) {
            LOG.error("Failed to load schedule state of version {}, this is possibly a bug, pls check coordinator log !", version);
            return;
        }
        Map<String, StreamDefinition> sds = getStreams(state.getStreamSnapshots());
        switch (type) {
        case ALERT_BOLT:
            // we might query metadata service query get metadata snapshot and StreamDefinition
            AlertBoltSpec alertSpec = state.getAlertSpecs().get(topologyId);
            if (alertSpec == null) {
                LOG.error(" alert spec for version {} not found for topology {} !", version, topologyId);
            } else {
                prePopulate(alertSpec, state.getPolicySnapshots());
                notifyAlertBolt(alertSpec, sds);
            }
            break;
        case ALERT_PUBLISH_BOLT:
            PublishSpec pubSpec = state.getPublishSpecs().get(topologyId);
            if (pubSpec == null) {
                LOG.error(" alert spec for version {} not found for topology {} !", version, topologyId);
            } else {
                notifyAlertPublishBolt(pubSpec, sds);
            }
            break;
        case SPOUT:
            SpoutSpec spoutSpec = state.getSpoutSpecs().get(topologyId);
            if (spoutSpec == null) {
                LOG.error(" alert spec for version {} not found for topology {} !", version, topologyId);
            } else {
                notifySpout(spoutSpec, sds);
            }
            break;
        case STREAM_ROUTER_BOLT:
            RouterSpec gSpec = state.getGroupSpecs().get(topologyId);
            if (gSpec == null) {
                LOG.error(" alert spec for version {} not found for topology {} !", version, topologyId);
            } else {
                notifyStreamRouterBolt(gSpec, sds);
            }
            break;
        default:
            LOG.error("unexpected metadata type: {} ", type);
        }
    }

    private void prePopulate(AlertBoltSpec alertSpec, List<VersionedPolicyDefinition> list) {
        Map<String, PolicyDefinition> policyMap = listToMap(list);
        for (Entry<String, List<String>> policyEntry : alertSpec.getBoltPolicyIdsMap().entrySet()) {
            List<PolicyDefinition> pds = alertSpec.getBoltPoliciesMap().get(policyEntry.getKey());
            if (pds == null) {
                pds = new ArrayList<PolicyDefinition>();
                alertSpec.getBoltPoliciesMap().put(policyEntry.getKey(), pds);
            }
            for (String policyName : policyEntry.getValue()) {
                if (policyMap.containsKey(policyName)) {
                    pds.add(policyMap.get(policyName));
                }
            }
        }
    }

    private Map<String, StreamDefinition> getStreams(List<VersionedStreamDefinition> streamSnapshots) {
        Map<String, StreamDefinition> result = new HashMap<String, StreamDefinition>();
        for (VersionedStreamDefinition vsd : streamSnapshots) {
            result.put(vsd.getDefinition().getStreamId(), vsd.getDefinition());
        }
        return result;
    }

    private Map<String, PolicyDefinition> listToMap(List<VersionedPolicyDefinition> listStreams) {
        Map<String, PolicyDefinition> result = new HashMap<String, PolicyDefinition>();
        for (VersionedPolicyDefinition sd : listStreams) {
            result.put(sd.getDefinition().getName(), sd.getDefinition());
        }
        return result;
    }

}