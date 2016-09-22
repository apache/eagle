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

package org.apache.eagle.alert.engine.spark.function;

import com.typesafe.config.Config;
import kafka.message.MessageAndMetadata;
import org.apache.eagle.alert.coordination.model.AlertBoltSpec;
import org.apache.eagle.alert.coordination.model.PublishSpec;
import org.apache.eagle.alert.coordination.model.RouterSpec;
import org.apache.eagle.alert.coordination.model.SpoutSpec;
import org.apache.eagle.alert.engine.coordinator.StreamDefinition;
import org.apache.eagle.alert.engine.spark.model.PolicyState;
import org.apache.eagle.alert.engine.spark.model.PublishState;
import org.apache.eagle.alert.engine.spark.model.RouteState;
import org.apache.eagle.alert.engine.spark.model.WindowState;
import org.apache.eagle.alert.service.SpecMetadataServiceClientImpl;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.kafka.HasOffsetRanges;
import org.apache.spark.streaming.kafka.OffsetRange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.eagle.alert.engine.utils.SpecUtils.getTopicsBySpoutSpec;

public class ProcessSpecFunction implements Function<JavaRDD<MessageAndMetadata<String, String>>, JavaRDD<MessageAndMetadata<String, String>>> {

    private static final Logger LOG = LoggerFactory.getLogger(ProcessSpecFunction.class);

    private SpecMetadataServiceClientImpl client;
    private AtomicReference<OffsetRange[]> offsetRanges;
    private AtomicReference<SpoutSpec> spoutSpecRef;
    private AtomicReference<PublishSpec> publishSpecRef;
    private AtomicReference<RouterSpec> routerSpecRef;
    private AtomicReference<AlertBoltSpec> alertBoltSpecRef;
    private AtomicReference<Map<String, StreamDefinition>> sdsRef;
    private AtomicReference<HashSet<String>> topicsRef;


    private RouteState routeState;
    private WindowState winstate;
    private PolicyState policyState;
    private PublishState publishState;

    public ProcessSpecFunction(AtomicReference<OffsetRange[]> offsetRanges, AtomicReference<SpoutSpec> spoutSpecRef,
                               AtomicReference<Map<String, StreamDefinition>> sdsRef, AtomicReference<AlertBoltSpec> alertBoltSpecRef,
                               AtomicReference<HashSet<String>> topicsRef, AtomicReference<RouterSpec> routerSpecRef,
                               Config config, WindowState windowState, RouteState routeState,
                               PolicyState policyState, PublishState publishState, AtomicReference<PublishSpec> publishSpecRef
    ) {
        this.offsetRanges = offsetRanges;
        this.spoutSpecRef = spoutSpecRef;
        this.alertBoltSpecRef = alertBoltSpecRef;
        this.routerSpecRef = routerSpecRef;
        this.publishSpecRef = publishSpecRef;
        this.topicsRef = topicsRef;
        this.client = new SpecMetadataServiceClientImpl(config);
        this.sdsRef = sdsRef;
        this.winstate = windowState;
        this.routeState = routeState;
        this.policyState = policyState;
        this.publishState = publishState;
    }

    @Override
    public JavaRDD<MessageAndMetadata<String, String>> call(JavaRDD<MessageAndMetadata<String, String>> rdd) throws Exception {

        SpoutSpec spoutSpec = client.getSpoutSpec();
        spoutSpecRef.set(spoutSpec);
        alertBoltSpecRef.set(client.getAlertBoltSpec());
        sdsRef.set(client.getSds());
        publishSpecRef.set(client.getPublishSpec());
        routerSpecRef.set(client.getRouterSpec());

        loadTopics(spoutSpec);
        updateOffsetRanges(rdd);
        recoverState();

        return rdd;
    }

    private void recoverState() {
        winstate.recover();
        routeState.recover();
        policyState.recover();
        publishState.recover();
    }

    private void updateOffsetRanges(JavaRDD<MessageAndMetadata<String, String>> rdd) {
        OffsetRange[] offsets = ((HasOffsetRanges) rdd.rdd()).offsetRanges();
        offsetRanges.set(offsets);
    }


    private void loadTopics(SpoutSpec spoutSpec) {
        Set<String> newTopics = getTopicsBySpoutSpec(spoutSpec);
        Set<String> oldTopics = topicsRef.get();
        LOG.info("Topics were old={}, new={}", oldTopics, newTopics);
        topicsRef.set(new HashSet<>(newTopics));
    }

}
