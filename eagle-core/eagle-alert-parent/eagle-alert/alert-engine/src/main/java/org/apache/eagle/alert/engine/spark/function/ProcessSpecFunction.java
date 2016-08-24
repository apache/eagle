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

import static org.apache.eagle.alert.engine.utils.SpecUtils.getTopicsByClient;

import org.apache.commons.collections.CollectionUtils;
import org.apache.eagle.alert.coordination.model.*;
import org.apache.eagle.alert.engine.coordinator.*;
import org.apache.eagle.alert.engine.runner.MapComparator;
import org.apache.eagle.alert.engine.spark.model.PolicyState;
import org.apache.eagle.alert.engine.spark.model.RouteState;
import org.apache.eagle.alert.engine.spark.model.WindowState;
import org.apache.eagle.alert.engine.utils.Constants;
import org.apache.eagle.alert.service.SpecMetadataServiceClientImpl;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.kafka.HasOffsetRanges;
import org.apache.spark.streaming.kafka.OffsetRange;
import com.typesafe.config.Config;
import kafka.message.MessageAndMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

public class ProcessSpecFunction implements Function<JavaRDD<MessageAndMetadata<String, String>>, JavaRDD<MessageAndMetadata<String, String>>> {

    private static final Logger LOG = LoggerFactory.getLogger(ProcessSpecFunction.class);

    private SpecMetadataServiceClientImpl client;
    private AtomicReference<OffsetRange[]> offsetRanges;
    private AtomicReference<SpoutSpec> spoutSpecRef;
    private AtomicReference<AlertBoltSpec> alertBoltSpecRef;
    private AtomicReference<Map<String, StreamDefinition>> sdsRef;
    private AtomicReference<HashSet<String>> topicsRef;

    private AtomicReference<Map<StreamPartition, StreamSortSpec>> cachedSSSRef;
    private AtomicReference<Map<StreamPartition, StreamRouterSpec>> cachedSRSRef;
    private AtomicReference<Map<String, Map<StreamPartition, StreamSortSpec>>> sssChangInfoRef;
    private AtomicReference<Map<String, Collection<StreamRouterSpec>>> srsChangInfoRef;

    private AtomicReference<Map<String, Publishment>> cachedPublishmentsRef;
    private AtomicReference<Map<String, List<Publishment>>> publishmentsChangInfoRef;


    private RouteState routeState;
    private WindowState winstate;
    private PolicyState policyState;

    public ProcessSpecFunction(AtomicReference<OffsetRange[]> offsetRanges, AtomicReference<SpoutSpec> spoutSpecRef,
                               AtomicReference<Map<String, StreamDefinition>> sdsRef, AtomicReference<AlertBoltSpec> alertBoltSpecRef,
                               AtomicReference<HashSet<String>> topicsRef, Config config, AtomicReference<Map<StreamPartition, StreamSortSpec>> sssRef,
                               AtomicReference<Map<StreamPartition, StreamRouterSpec>> srsRef, AtomicReference<Map<String, Map<StreamPartition, StreamSortSpec>>> sssChangInfoRef,
                               AtomicReference<Map<String, Collection<StreamRouterSpec>>> srsChangInfoRef, WindowState windowState, RouteState routeState,
                               PolicyState policyState, AtomicReference<Map<String, Publishment>> cachedPublishmentsRef,
                               AtomicReference<Map<String, List<Publishment>>> publishmentsChangInfoRef
    ) {
        this.offsetRanges = offsetRanges;
        this.spoutSpecRef = spoutSpecRef;
        this.alertBoltSpecRef = alertBoltSpecRef;
        this.topicsRef = topicsRef;
        this.client = new SpecMetadataServiceClientImpl(config);
        this.sdsRef = sdsRef;
        this.cachedSSSRef = sssRef;
        this.cachedSRSRef = srsRef;
        this.sssChangInfoRef = sssChangInfoRef;
        this.srsChangInfoRef = srsChangInfoRef;
        this.winstate = windowState;
        this.routeState = routeState;
        this.policyState = policyState;
        this.cachedPublishmentsRef = cachedPublishmentsRef;
        this.publishmentsChangInfoRef = publishmentsChangInfoRef;
    }

    @Override
    public JavaRDD<MessageAndMetadata<String, String>> call(JavaRDD<MessageAndMetadata<String, String>> rdd) throws Exception {

        spoutSpecRef.set(client.getSpoutSpec());
        alertBoltSpecRef.set(client.getAlertBoltSpec());
        sdsRef.set(client.getSds());


        loadTopics();
        processRouteBoltSpecChange();
        processPublishSpecChange();
        updateOffsetRanges(rdd);
        recoverState();

        return rdd;
    }

    private void recoverState() {
        winstate.recover();
        routeState.recover();
        policyState.recover();
    }

    private void updateOffsetRanges(JavaRDD<MessageAndMetadata<String, String>> rdd) {
        OffsetRange[] offsets = ((HasOffsetRanges) rdd.rdd()).offsetRanges();
        offsetRanges.set(offsets);
    }


    private void loadTopics() {
        Set<String> newTopics = getTopicsByClient(client);
        Set<String> oldTopics = topicsRef.get();
        LOG.info("Topics were old={}, new={}", oldTopics, newTopics);
        topicsRef.set(new HashSet<>(newTopics));
    }

    private void processPublishSpecChange() {

        PublishSpec pubSpec = client.getPublishSpec();
        if (pubSpec == null) {
            return;
        }

        List<Publishment> newPublishments = pubSpec.getPublishments();
        if (newPublishments == null) {
            LOG.info("no publishments with PublishSpec {} for this topology", pubSpec);
            return;
        }
        final Map<String, Publishment> cachedPublishments = cachedPublishmentsRef.get();
        LOG.info("cachedPublishmentsRef" + cachedPublishments);
        Map<String, Publishment> newPublishmentsMap = new HashMap<>();
        newPublishments.forEach(p -> newPublishmentsMap.put(p.getName(), p));
        MapComparator<String, Publishment> comparator = new MapComparator<>(newPublishmentsMap, cachedPublishments);
        comparator.compare();

        List<Publishment> beforeModified = new ArrayList<>();
        comparator.getModified().forEach(p -> beforeModified.add(cachedPublishments.get(p.getName())));
        Map<String, List<Publishment>> publishmentsChangInfo = new HashMap<>();
        publishmentsChangInfo.put(Constants.ADDED, comparator.getAdded());
        publishmentsChangInfo.put(Constants.REMOVED, comparator.getRemoved());
        publishmentsChangInfo.put(Constants.MODIFIED, comparator.getModified());
        publishmentsChangInfo.put(Constants.BEFORE_MODIFIED, beforeModified);
        publishmentsChangInfoRef.set(publishmentsChangInfo);

        // switch
        cachedPublishmentsRef.set(newPublishmentsMap);
    }

    private void processRouteBoltSpecChange() {

        RouterSpec spec = client.getRouterSpec();
        // mapping from StreamPartition to StreamSortSpec
        final Map<StreamPartition, StreamSortSpec> cachedSSS = cachedSSSRef.get();
        // mapping from StreamPartition(streamId, groupbyspec) to StreamRouterSpec
        final Map<StreamPartition, StreamRouterSpec> cachedSRS = cachedSRSRef.get();

        // figure out added, removed, modified StreamSortSpec
        Map<StreamPartition, StreamSortSpec> newSSS = new HashMap<>();
        spec.getRouterSpecs().forEach(t -> {
            if (t.getPartition().getSortSpec() != null) {
                newSSS.put(t.getPartition(), t.getPartition().getSortSpec());
            }
        });

        Set<StreamPartition> newStreamIds = newSSS.keySet();
        Set<StreamPartition> cachedStreamIds = cachedSSS.keySet();
        Collection<StreamPartition> addedStreamIds = CollectionUtils.subtract(newStreamIds, cachedStreamIds);
        Collection<StreamPartition> removedStreamIds = CollectionUtils.subtract(cachedStreamIds, newStreamIds);
        Collection<StreamPartition> modifiedStreamIds = CollectionUtils.intersection(newStreamIds, cachedStreamIds);

        Map<StreamPartition, StreamSortSpec> added = new HashMap<>();
        Map<StreamPartition, StreamSortSpec> removed = new HashMap<>();
        Map<StreamPartition, StreamSortSpec> modified = new HashMap<>();
        addedStreamIds.forEach(s -> added.put(s, newSSS.get(s)));
        removedStreamIds.forEach(s -> removed.put(s, cachedSSS.get(s)));
        modifiedStreamIds.forEach(s -> {
            if (!newSSS.get(s).equals(cachedSSS.get(s))) { // this means StreamSortSpec is changed for one specific streamId
                modified.put(s, newSSS.get(s));
            }
        });
        LOG.info("added StreamSortSpec " + added);
        LOG.info("removed StreamSortSpec " + removed);
        LOG.info("modified StreamSortSpec " + modified);
        Map<String, Map<StreamPartition, StreamSortSpec>> sssChangInfo = new HashMap<>();
        sssChangInfo.put(Constants.ADDED, added);
        sssChangInfo.put(Constants.REMOVED, removed);
        sssChangInfo.put(Constants.MODIFIED, modified);
        sssChangInfoRef.set(sssChangInfo);
        // switch cache
        cachedSSSRef.set(newSSS);

        // figure out added, removed, modified StreamRouterSpec
        Map<StreamPartition, StreamRouterSpec> newSRS = new HashMap<>();
        spec.getRouterSpecs().forEach(t -> newSRS.put(t.getPartition(), t));

        Set<StreamPartition> newStreamPartitions = newSRS.keySet();
        Set<StreamPartition> cachedStreamPartitions = cachedSRS.keySet();

        Collection<StreamPartition> addedStreamPartitions = CollectionUtils.subtract(newStreamPartitions, cachedStreamPartitions);
        Collection<StreamPartition> removedStreamPartitions = CollectionUtils.subtract(cachedStreamPartitions, newStreamPartitions);
        Collection<StreamPartition> modifiedStreamPartitions = CollectionUtils.intersection(newStreamPartitions, cachedStreamPartitions);

        Collection<StreamRouterSpec> addedRouterSpecs = new ArrayList<>();
        Collection<StreamRouterSpec> removedRouterSpecs = new ArrayList<>();
        Collection<StreamRouterSpec> modifiedRouterSpecs = new ArrayList<>();
        addedStreamPartitions.forEach(s -> addedRouterSpecs.add(newSRS.get(s)));
        removedStreamPartitions.forEach(s -> removedRouterSpecs.add(cachedSRS.get(s)));
        modifiedStreamPartitions.forEach(s -> {
            if (!newSRS.get(s).equals(cachedSRS.get(s))) { // this means StreamRouterSpec is changed for one specific StreamPartition
                modifiedRouterSpecs.add(newSRS.get(s));
            }
        });

        LOG.info("added StreamRouterSpec " + addedRouterSpecs);
        LOG.info("removed StreamRouterSpec " + removedRouterSpecs);
        LOG.info("modified StreamRouterSpec " + modifiedRouterSpecs);
        Map<String, Collection<StreamRouterSpec>> srsChangInfo = new HashMap<>();
        srsChangInfo.put(Constants.ADDED, addedRouterSpecs);
        srsChangInfo.put(Constants.REMOVED, removedRouterSpecs);
        srsChangInfo.put(Constants.MODIFIED, modifiedRouterSpecs);
        srsChangInfoRef.set(srsChangInfo);

        // switch cache
        cachedSRSRef.set(newSRS);
    }

}
