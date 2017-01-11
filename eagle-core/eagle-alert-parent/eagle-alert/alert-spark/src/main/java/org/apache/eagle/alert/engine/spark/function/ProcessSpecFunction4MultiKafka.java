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
import org.apache.eagle.alert.coordination.model.*;
import org.apache.eagle.alert.engine.coordinator.StreamDefinition;
import org.apache.eagle.alert.engine.runner.UnitSparkTopologyRunner4MultiKafka;
import org.apache.eagle.alert.engine.spark.manager.SpecManager;
import org.apache.eagle.alert.engine.spark.model.*;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.kafka.HasOffsetRanges;
import org.apache.spark.streaming.kafka.OffsetRange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.atomic.AtomicReference;


public class ProcessSpecFunction4MultiKafka implements Function<JavaRDD<MessageAndMetadata<String, String>>, JavaRDD<MessageAndMetadata<String, String>>> {

    private static final Logger LOG = LoggerFactory.getLogger(ProcessSpecFunction4MultiKafka.class);
    private static final long serialVersionUID = 381513979960046346L;

    private AtomicReference<SpoutSpec> spoutSpecRef;
    private AtomicReference<PublishSpec> publishSpecRef;
    private AtomicReference<RouterSpec> routerSpecRef;
    private AtomicReference<AlertBoltSpec> alertBoltSpecRef;
    private AtomicReference<Map<String, StreamDefinition>> sdsRef;
    private AtomicReference<Map<KafkaClusterInfo,Set<String>>> clusterInfoRef;


    private RouteState routeState;
    private WindowState winstate;
    private PolicyState policyState;
    private PublishState publishState;
    private SiddhiState siddhiState;

    private int numOfAlertBolts;
    private Config config;

    public ProcessSpecFunction4MultiKafka(AtomicReference<SpoutSpec> spoutSpecRef,
                                          AtomicReference<Map<String, StreamDefinition>> sdsRef, AtomicReference<AlertBoltSpec> alertBoltSpecRef,
                                          AtomicReference<PublishSpec> publishSpecRef, AtomicReference<Map<KafkaClusterInfo,Set<String>>> clusterInfoRef,
                                          AtomicReference<RouterSpec> routerSpecRef, Config config, WindowState windowState,
                                          RouteState routeState, PolicyState policyState, PublishState publishState, SiddhiState siddhiState,
                                          int numOfAlertBolts
    ) {
        this.spoutSpecRef = spoutSpecRef;
        this.alertBoltSpecRef = alertBoltSpecRef;
        this.routerSpecRef = routerSpecRef;
        this.publishSpecRef = publishSpecRef;
        this.clusterInfoRef = clusterInfoRef;
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

        // reloadCluster(spoutSpec);
        recoverState();

        return rdd;
    }

    private void recoverState() {
        winstate.recover();
        routeState.recover();
        policyState.recover();
        publishState.recover();
        siddhiState.recover();
    }


    private void reloadCluster(SpoutSpec spoutSpec) {
        //Set<String> newTopics = getTopicsBySpoutSpec(spoutSpec);
        //Set<String> oldTopics = topicsRef.get();
        //LOG.info("Topics were old={}, new={}", oldTopics, newTopics);
        //topicsRef.set(new HashSet<>(newTopics));
        // general new clusterinfo from old cache
        Map<String, Map<String, String>> dataSourceProperties = new HashMap<>();
        Collection<Kafka2TupleMetadata> kafka2TupleMetadataList = spoutSpec.getKafka2TupleMetadataMap().values();
        for (Kafka2TupleMetadata ds : kafka2TupleMetadataList) {
            //ds.getProperties().put("spout.kafkaBrokerZkQuorum", "localhost:2181");
            dataSourceProperties.put(ds.getTopic(), ds.getProperties());
        }
        Map<KafkaClusterInfo, Set<String>> newClusterInfo = UnitSparkTopologyRunner4MultiKafka.getKafkaClustersByKafkaInfo(dataSourceProperties, clusterInfoRef.get());
        clusterInfoRef.set(newClusterInfo);
    }

}