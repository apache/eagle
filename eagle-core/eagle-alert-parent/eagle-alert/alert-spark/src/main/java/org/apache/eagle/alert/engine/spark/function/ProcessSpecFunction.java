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
import org.apache.eagle.alert.engine.spark.manager.SpecManager;
import org.apache.eagle.alert.engine.spark.model.*;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.kafka.HasOffsetRanges;
import org.apache.spark.streaming.kafka.OffsetRange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;


public class ProcessSpecFunction implements Function<JavaRDD<MessageAndMetadata<String, String>>, JavaRDD<MessageAndMetadata<String, String>>> {

    private static final Logger LOG = LoggerFactory.getLogger(ProcessSpecFunction.class);
    private static final long serialVersionUID = 381513979960046346L;

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
    private SiddhiState siddhiState;

    private int numOfAlertBolts;
    private Config config;

    public ProcessSpecFunction(AtomicReference<OffsetRange[]> offsetRanges, AtomicReference<SpoutSpec> spoutSpecRef,
                               AtomicReference<Map<String, StreamDefinition>> sdsRef, AtomicReference<AlertBoltSpec> alertBoltSpecRef,
                               AtomicReference<PublishSpec> publishSpecRef, AtomicReference<HashSet<String>> topicsRef,
                               AtomicReference<RouterSpec> routerSpecRef, Config config, WindowState windowState,
                               RouteState routeState, PolicyState policyState, PublishState publishState, SiddhiState siddhiState,
                               int numOfAlertBolts
    ) {
        this.offsetRanges = offsetRanges;
        this.spoutSpecRef = spoutSpecRef;
        this.alertBoltSpecRef = alertBoltSpecRef;
        this.routerSpecRef = routerSpecRef;
        this.publishSpecRef = publishSpecRef;
        this.topicsRef = topicsRef;
        this.sdsRef = sdsRef;
        this.winstate = windowState;
        this.routeState = routeState;
        this.policyState = policyState;
        this.publishState = publishState;
        this.siddhiState = siddhiState;
        this.config = config;
        this.numOfAlertBolts = numOfAlertBolts;
    }

    @Override
    public JavaRDD<MessageAndMetadata<String, String>> call(JavaRDD<MessageAndMetadata<String, String>> rdd) throws Exception {
        SpecManager specManager = new SpecManager(config, numOfAlertBolts);
        SpoutSpec spoutSpec = specManager.generateSpout();
        spoutSpecRef.set(spoutSpec);
        AlertBoltSpec alertBoltSpec = specManager.generateAlertBoltSpec();
        alertBoltSpecRef.set(alertBoltSpec);
        //Fix always get getModified policy
        alertBoltSpec.getBoltPoliciesMap().values().forEach((policyDefinitions) -> policyDefinitions.forEach(policy -> {
                    policy.getDefinition().setInputStreams(policy.getInputStreams());
                    policy.getDefinition().setOutputStreams(policy.getOutputStreams());
                }
                )
        );
        sdsRef.set(specManager.generateSds());
        publishSpecRef.set(specManager.generatePublishSpec());
        routerSpecRef.set(specManager.generateRouterSpec());

        loadTopics(spoutSpec);
        updateOffsetRanges(rdd);
        recoverState(rdd);

        return rdd;
    }

    private void recoverState(JavaRDD<MessageAndMetadata<String, String>> rdd) {
        winstate.recover(rdd);
        routeState.recover(rdd);
        policyState.recover(rdd);
        publishState.recover(rdd);
        siddhiState.recover(rdd);
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


    private Set<String> getTopicsBySpoutSpec(SpoutSpec spoutSpec) {
        if (spoutSpec == null) {
            return Collections.emptySet();
        }
        return spoutSpec.getKafka2TupleMetadataMap().keySet();
    }

}