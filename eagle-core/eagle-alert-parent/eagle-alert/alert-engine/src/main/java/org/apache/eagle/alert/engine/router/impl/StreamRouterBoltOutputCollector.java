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
package org.apache.eagle.alert.engine.router.impl;

import org.apache.eagle.alert.coordination.model.PolicyWorkerQueue;
import org.apache.eagle.alert.coordination.model.StreamRouterSpec;
import org.apache.eagle.alert.coordination.model.WorkSlot;
import org.apache.eagle.alert.engine.PartitionedEventCollector;
import org.apache.eagle.alert.engine.StreamContext;
import org.apache.eagle.alert.engine.coordinator.StreamDefinition;
import org.apache.eagle.alert.engine.coordinator.StreamPartition;
import org.apache.eagle.alert.engine.model.PartitionedEvent;
import org.apache.eagle.alert.engine.model.StreamEvent;
import org.apache.eagle.alert.engine.router.*;
import org.apache.eagle.alert.utils.StreamIdConversion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * After sorting, one stream's message will be routed based on its StreamPartition
 * One stream may have multiple StreamPartitions based on how this stream is grouped by.
 * TODO: Add metric statistics
 */
public class StreamRouterBoltOutputCollector implements PartitionedEventCollector, StreamRouteSpecListener {
    private static final Logger LOG = LoggerFactory.getLogger(StreamRouterBoltOutputCollector.class);
    private final StreamOutputCollector outputCollector;
    private final Object outputLock = new Object();
    //    private final List<String> outputStreamIds;
    private final StreamContext streamContext;
    private volatile Map<StreamPartition, List<StreamRouterSpec>> routeSpecMap;
    private volatile Map<StreamPartition, List<StreamRoutePartitioner>> routePartitionerMap;
    private final String sourceId;

    public StreamRouterBoltOutputCollector(String sourceId, StreamOutputCollector outputCollector, List<String> outputStreamIds, StreamContext streamContext) {
        this.sourceId = sourceId;
        this.outputCollector = outputCollector;
        this.routeSpecMap = new HashMap<>();
        this.routePartitionerMap = new HashMap<>();
        // this.outputStreamIds = outputStreamIds;
        this.streamContext = streamContext;
    }

    public StreamRouterBoltOutputCollector(String sourceId, StreamOutputCollector outputCollector, StreamContext streamContext,
                                           Map<StreamPartition, List<StreamRouterSpec>> routeSpecMap,
                                           Map<StreamPartition, List<StreamRoutePartitioner>> routePartitionerMap) {
        this.sourceId = sourceId;
        this.outputCollector = outputCollector;
        this.routeSpecMap = routeSpecMap;
        this.routePartitionerMap = routePartitionerMap;
        // this.outputStreamIds = outputStreamIds;
        this.streamContext = streamContext;
    }

    public void emit(PartitionedEvent event) {
        try {
            this.streamContext.counter().incr("send_count");
            StreamPartition partition = event.getPartition();
            List<StreamRouterSpec> routerSpecs = routeSpecMap.get(partition);
            if (routerSpecs == null || routerSpecs.size() <= 0) {
                if (LOG.isDebugEnabled()) {
                    // Don't know how to route stream, if it's correct, it's better to filter useless stream in spout side
                    LOG.debug("Drop event {} as StreamPartition {} is not pointed to any router metadata {}", event, event.getPartition(), routeSpecMap);
                }
                this.drop(event);
                return;
            }

            if (routePartitionerMap.get(partition) == null) {
                LOG.error("Partitioner for " + routerSpecs.get(0) + " is null");
                synchronized (outputLock) {
                    this.streamContext.counter().incr("fail_count");
                    this.outputCollector.fail(event);
                }
                return;
            }

            StreamEvent newEvent = event.getEvent().copy();

            // Get handler for the partition
            List<StreamRoutePartitioner> queuePartitioners = routePartitionerMap.get(partition);

            synchronized (outputLock) {
                for (StreamRoutePartitioner queuePartitioner : queuePartitioners) {
                    List<StreamRoute> streamRoutes = queuePartitioner.partition(newEvent);
                    // it is possible that one event can be sent to multiple slots in one slotqueue if that is All grouping
                    for (StreamRoute streamRoute : streamRoutes) {
                        String targetStreamId = StreamIdConversion.generateStreamIdBetween(sourceId, streamRoute.getTargetComponentId());
                        try {
                            PartitionedEvent emittedEvent = new PartitionedEvent(newEvent, partition, streamRoute.getPartitionKey());
                            // Route Target Stream id instead of component id
                            if (LOG.isDebugEnabled()) {
                                LOG.debug("Emitted to stream {} with message {}", targetStreamId, emittedEvent);
                            }
                            outputCollector.emit(targetStreamId, event);
                            this.streamContext.counter().incr("emit_count");
                        } catch (RuntimeException ex) {
                            this.streamContext.counter().incr("fail_count");
                            LOG.error("Failed to emit to {} with {}", targetStreamId, newEvent, ex);
                            throw ex;
                        }
                    }
                }
                outputCollector.ack(event);
            }
        } catch (Exception ex) {
            LOG.error(ex.getMessage(), ex);
            synchronized (outputLock) {
                this.streamContext.counter().incr("fail_count");
                this.outputCollector.fail(event);
            }
        }
    }

    @Override
    public void onStreamRouterSpecChange(Collection<StreamRouterSpec> added,
                                         Collection<StreamRouterSpec> removed,
                                         Collection<StreamRouterSpec> modified,
                                         Map<String, StreamDefinition> sds) {
        Map<StreamPartition, List<StreamRouterSpec>> copyRouteSpecMap = new HashMap<>(routeSpecMap);
        Map<StreamPartition, List<StreamRoutePartitioner>> copyRoutePartitionerMap = new HashMap<>(routePartitionerMap);

        // added StreamRouterSpec i.e. there is a new StreamPartition
        for (StreamRouterSpec spec : added) {
            if (copyRouteSpecMap.containsKey(spec.getPartition())
                    && copyRouteSpecMap.get(spec.getPartition()).contains(spec)) {
                LOG.error("Metadata calculation error: add existing StreamRouterSpec " + spec);
            } else {
                inplaceAdd(copyRouteSpecMap, copyRoutePartitionerMap, spec, sds);
            }
        }

        // removed StreamRouterSpec i.e. there is a deleted StreamPartition
        for (StreamRouterSpec spec : removed) {
            if (!copyRouteSpecMap.containsKey(spec.getPartition())
                    || !copyRouteSpecMap.get(spec.getPartition()).contains(spec)) {
                LOG.error("Metadata calculation error: remove non-existing StreamRouterSpec " + spec);
            } else {
                inplaceRemove(copyRouteSpecMap, copyRoutePartitionerMap, spec);
            }
        }

        // modified StreamRouterSpec, i.e. there is modified StreamPartition, for example WorkSlotQueue assignment is changed
        for (StreamRouterSpec spec : modified) {
            if (!copyRouteSpecMap.containsKey(spec.getPartition())
                    || copyRouteSpecMap.get(spec.getPartition()).contains(spec)) {
                LOG.error("Metadata calculation error: modify nonexisting StreamRouterSpec " + spec);
            } else {
                inplaceRemove(copyRouteSpecMap, copyRoutePartitionerMap, spec);
                inplaceAdd(copyRouteSpecMap, copyRoutePartitionerMap, spec, sds);
            }
        }

        // switch
        routeSpecMap = copyRouteSpecMap;
        routePartitionerMap = copyRoutePartitionerMap;
    }

    private void inplaceRemove(Map<StreamPartition, List<StreamRouterSpec>> routeSpecMap,
                               Map<StreamPartition, List<StreamRoutePartitioner>> routePartitionerMap,
                               StreamRouterSpec toBeRemoved) {
        routeSpecMap.remove(toBeRemoved.getPartition());
        routePartitionerMap.remove(toBeRemoved.getPartition());
    }

    private void inplaceAdd(Map<StreamPartition, List<StreamRouterSpec>> routeSpecMap,
                            Map<StreamPartition, List<StreamRoutePartitioner>> routePartitionerMap,
                            StreamRouterSpec toBeAdded, Map<String, StreamDefinition> sds) {
        if (!routeSpecMap.containsKey(toBeAdded.getPartition())) {
            routeSpecMap.put(toBeAdded.getPartition(), new ArrayList<StreamRouterSpec>());
        }
        routeSpecMap.get(toBeAdded.getPartition()).add(toBeAdded);
        try {
            List<StreamRoutePartitioner> routePartitioners = calculatePartitioner(toBeAdded, sds, routePartitionerMap);
            routePartitionerMap.put(toBeAdded.getPartition(), routePartitioners);
        } catch (Exception e) {
            LOG.error("ignore this failure StreamRouterSpec " + toBeAdded + ", with error" + e.getMessage(), e);
            routeSpecMap.remove(toBeAdded.getPartition());
            routePartitionerMap.remove(toBeAdded.getPartition());
        }
    }

    private List<StreamRoutePartitioner> calculatePartitioner(StreamRouterSpec streamRouterSpec,
                                                              Map<String, StreamDefinition> sds,
                                                              Map<StreamPartition, List<StreamRoutePartitioner>> routePartitionerMap) throws Exception {
        List<StreamRoutePartitioner> routePartitioners = routePartitionerMap.get(streamRouterSpec.getPartition());
        if (routePartitioners == null) {
            routePartitioners = new ArrayList<>();
        }

        for (PolicyWorkerQueue pwq : streamRouterSpec.getTargetQueue()) {
            List<String> bolts = new ArrayList<>();
            for (WorkSlot work : pwq.getWorkers()) {
                bolts.add(work.getBoltId());
            }
            routePartitioners.add(StreamRoutePartitionFactory.createRoutePartitioner(
                    bolts,
                    sds.get(streamRouterSpec.getPartition().getStreamId()),
                    streamRouterSpec.getPartition()));
        }
        return routePartitioners;
    }

    @Override
    public void drop(PartitionedEvent event) {
        synchronized (outputLock) {
            this.streamContext.counter().incr("drop_count");
            if (event.getAnchor() != null) {
                this.outputCollector.ack(event);
            } else {
                //TODO PartitionedEvent NOT USE TUPLE
                //throw new IllegalStateException(event.toString() + " was not acked as anchor is null");
            }
        }
    }

    public List flush() {
        if (this.outputCollector instanceof SparkOutputCollector) {
            SparkOutputCollector sparkOutputCollector = (SparkOutputCollector) outputCollector;
            return sparkOutputCollector.flushPartitionedEvent();
        }
        return Collections.emptyList();
    }

    public Map<StreamPartition, List<StreamRouterSpec>> getRouteSpecMap() {
        return this.routeSpecMap;
    }

    public Map<StreamPartition, List<StreamRoutePartitioner>> getRoutePartitionerMap() {
        return this.routePartitionerMap;
    }
}