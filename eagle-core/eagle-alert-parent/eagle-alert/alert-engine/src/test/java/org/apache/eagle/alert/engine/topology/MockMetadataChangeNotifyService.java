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

package org.apache.eagle.alert.engine.topology;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.eagle.alert.coordination.model.AlertBoltSpec;
import org.apache.eagle.alert.coordination.model.PublishSpec;
import org.apache.eagle.alert.coordination.model.RouterSpec;
import org.apache.eagle.alert.coordination.model.SpoutSpec;
import org.apache.eagle.alert.engine.coordinator.MetadataType;
import org.apache.eagle.alert.engine.coordinator.StreamDefinition;
import org.apache.eagle.alert.engine.coordinator.impl.AbstractMetadataChangeNotifyService;
import org.apache.eagle.alert.engine.utils.MetadataSerDeser;
import org.codehaus.jackson.type.TypeReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.typesafe.config.Config;

/**
 * Since 5/4/16.
 */
@SuppressWarnings( {"serial"})
public class MockMetadataChangeNotifyService extends AbstractMetadataChangeNotifyService implements Runnable {
    private final static Logger LOG = LoggerFactory.getLogger(MockMetadataChangeNotifyService.class);
    @SuppressWarnings("unused")
    private static final String[] topics = new String[] {"testTopic3", "testTopic4", "testTopic5"};
    @SuppressWarnings("unused")
    private String topologyName;
    @SuppressWarnings("unused")
    private String spoutId;
    private Map<String, StreamDefinition> sds;

    public MockMetadataChangeNotifyService(String topologyName, String spoutId) {
        this.topologyName = topologyName;
        this.spoutId = spoutId;
    }

    @Override
    public void init(Config config, MetadataType type) {
        super.init(config, type);
        this.sds = defineStreamDefinitions();
        new Thread(this).start();
    }

    @Override
    public void run() {
        switch (type) {
            case SPOUT:
                notifySpout(Arrays.asList("testTopic3", "testTopic4"), Arrays.asList("testTopic5"));
                break;
            case STREAM_ROUTER_BOLT:
                populateRouterMetadata();
                break;
            case ALERT_BOLT:
                populateAlertBoltSpec();
                break;
            case ALERT_PUBLISH_BOLT:
                notifyAlertPublishBolt();
                break;
            default:
                LOG.error("that is not possible man!");
        }
    }

    private Map<String, StreamDefinition> defineStreamDefinitions() {
        Map<String, StreamDefinition> sds = MetadataSerDeser.deserialize(getClass().getResourceAsStream("/testStreamDefinitionsSpec.json"),
            new TypeReference<Map<String, StreamDefinition>>() {
            });
        return sds;
    }

    private void notifySpout(List<String> plainStringTopics, List<String> jsonStringTopics) {
        SpoutSpec newSpec = MetadataSerDeser.deserialize(getClass().getResourceAsStream("/testSpoutSpec.json"), SpoutSpec.class);
        notifySpout(newSpec, sds);
    }

    private void populateRouterMetadata() {
        RouterSpec boltSpec = MetadataSerDeser.deserialize(getClass().getResourceAsStream("/testStreamRouterBoltSpec.json"), RouterSpec.class);
        notifyStreamRouterBolt(boltSpec, sds);
    }

    private void populateAlertBoltSpec() {
        AlertBoltSpec spec = MetadataSerDeser.deserialize(getClass().getResourceAsStream("/testAlertBoltSpec.json"), AlertBoltSpec.class);
        notifyAlertBolt(spec, sds);
    }

    private void notifyAlertPublishBolt() {
        PublishSpec spec = MetadataSerDeser.deserialize(getClass().getResourceAsStream("/testPublishSpec.json"), PublishSpec.class);
        notifyAlertPublishBolt(spec, sds);
    }
}
