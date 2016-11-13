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

package org.apache.eagle.alert.engine.router.impl;

import org.apache.eagle.alert.coordination.model.PolicyWorkerQueue;
import org.apache.eagle.alert.coordination.model.StreamRouterSpec;
import org.apache.eagle.alert.coordination.model.WorkSlot;
import org.apache.eagle.alert.engine.PartitionedEventCollector;
import org.apache.eagle.alert.engine.coordinator.StreamDefinition;
import org.apache.eagle.alert.engine.coordinator.StreamPartition;
import org.apache.eagle.alert.engine.model.PartitionedEvent;
import org.apache.eagle.alert.engine.model.StreamEvent;
import org.apache.eagle.alert.engine.router.StreamRoute;
import org.apache.eagle.alert.engine.router.StreamRoutePartitionFactory;
import org.apache.eagle.alert.engine.router.StreamRoutePartitioner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.Serializable;
import java.util.*;

public class SparkStreamRouterBoltOutputCollector implements PartitionedEventCollector, Serializable {
    private static final Logger LOG = LoggerFactory.getLogger(SparkStreamRouterBoltOutputCollector.class);
    private final List<Tuple2<Integer, PartitionedEvent>> outputCollector;
    private Map<StreamPartition, StreamRouterSpec> routeSpecMap;
    private Map<StreamPartition, List<StreamRoutePartitioner>> routePartitionerMap;

    public SparkStreamRouterBoltOutputCollector(Map<StreamPartition, StreamRouterSpec> routeSpecMap, Map<StreamPartition, List<StreamRoutePartitioner>> routePartitionerMap) {
        this.outputCollector = new ArrayList<>();
        this.routeSpecMap = routeSpecMap;
        this.routePartitionerMap = routePartitionerMap;
    }

    public List<Tuple2<Integer, PartitionedEvent>> emitResult() {
        return outputCollector;
    }

    public void emit(PartitionedEvent event) {
        try {
            StreamPartition partition = event.getPartition();
            StreamRouterSpec routerSpec = routeSpecMap.get(partition);
            if (routerSpec == null) {
                if (LOG.isDebugEnabled()) {
                    // Don't know how to route stream, if it's correct, it's better to filter useless stream in spout side
                    LOG.debug("Drop event {} as StreamPartition {} is not pointed to any router metadata {}", event, event.getPartition(), routeSpecMap);
                }
                this.drop(event);
                return;
            }

            if (routePartitionerMap.get(routerSpec.getPartition()) == null) {
                LOG.info("Partitioner for " + routerSpec + " is null");
                return;
            }

            StreamEvent newEvent = event.getEvent().copy();

            // Get handler for the partition
            List<StreamRoutePartitioner> queuePartitioners = routePartitionerMap.get(partition);

            //  synchronized (outputLock) {
            for (StreamRoutePartitioner queuePartitioner : queuePartitioners) {
                List<StreamRoute> streamRoutes = queuePartitioner.partition(newEvent);
                // it is possible that one event can be sent to multiple slots in one slotqueue if that is All grouping
                for (StreamRoute streamRoute : streamRoutes) {
                    int partitionIndex = getPartitionIndex(streamRoute);
                    try {
                        PartitionedEvent emittedEvent = new PartitionedEvent(newEvent, routerSpec.getPartition(), streamRoute.getPartitionKey());
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("Emitted to partition {} with message {}", partitionIndex, emittedEvent);
                        }
                        outputCollector.add(new Tuple2<>(partitionIndex, event));
                    } catch (RuntimeException ex) {
                        LOG.info("Failed to emit to partition {} with {}", partitionIndex, newEvent, ex);
                        throw ex;
                    }
                }
            }
        } catch (Exception ex) {
            LOG.info(ex.getMessage(), ex);
        }
    }

    private Integer getPartitionIndex(StreamRoute streamRoute) {
        return Integer.valueOf(streamRoute.getTargetComponentId().replaceAll("[^\\d.]", ""));
    }

    public void onStreamRouterSpecChange(Collection<StreamRouterSpec> added,
                                         Collection<StreamRouterSpec> removed,
                                         Collection<StreamRouterSpec> modified,
                                         Map<String, StreamDefinition> sds) {
        Map<StreamPartition, StreamRouterSpec> copyRouteSpecMap = new HashMap<>(routeSpecMap);
        Map<StreamPartition, List<StreamRoutePartitioner>> copyRoutePartitionerMap = new HashMap<>(routePartitionerMap);

        // added StreamRouterSpec i.e. there is a new StreamPartition
        for (StreamRouterSpec spec : added) {
            if (copyRouteSpecMap.containsKey(spec.getPartition())) {
                LOG.error("Metadata calculation error: add existing StreamRouterSpec " + spec);
            } else {
                inplaceAdd(copyRouteSpecMap, copyRoutePartitionerMap, spec, sds);
            }
        }

        // removed StreamRouterSpec i.e. there is a deleted StreamPartition
        for (StreamRouterSpec spec : removed) {
            if (!copyRouteSpecMap.containsKey(spec.getPartition())) {
                LOG.error("Metadata calculation error: remove non-existing StreamRouterSpec " + spec);
            } else {
                inplaceRemove(copyRouteSpecMap, copyRoutePartitionerMap, spec);
            }
        }

        // modified StreamRouterSpec, i.e. there is modified StreamPartition, for example WorkSlotQueue assignment is changed
        for (StreamRouterSpec spec : modified) {
            if (!copyRouteSpecMap.containsKey(spec.getPartition())) {
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

    private void inplaceRemove(Map<StreamPartition, StreamRouterSpec> routeSpecMap,
                               Map<StreamPartition, List<StreamRoutePartitioner>> routePartitionerMap,
                               StreamRouterSpec toBeRemoved) {
        routeSpecMap.remove(toBeRemoved.getPartition());
        routePartitionerMap.remove(toBeRemoved.getPartition());
    }

    private void inplaceAdd(Map<StreamPartition, StreamRouterSpec> routeSpecMap,
                            Map<StreamPartition, List<StreamRoutePartitioner>> routePartitionerMap,
                            StreamRouterSpec toBeAdded, Map<String, StreamDefinition> sds) {
        routeSpecMap.put(toBeAdded.getPartition(), toBeAdded);
        try {
            List<StreamRoutePartitioner> routePartitioners = calculatePartitioner(toBeAdded, sds);
            routePartitionerMap.put(toBeAdded.getPartition(), routePartitioners);
        } catch (Exception e) {
            LOG.error("ignore this failure StreamRouterSpec " + toBeAdded + ", with error" + e.getMessage(), e);
            routeSpecMap.remove(toBeAdded.getPartition());
            routePartitionerMap.remove(toBeAdded.getPartition());
        }
    }

    private List<StreamRoutePartitioner> calculatePartitioner(StreamRouterSpec streamRouterSpec, Map<String, StreamDefinition> sds) throws Exception {
        List<StreamRoutePartitioner> routePartitioners = new ArrayList<>();
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
    }

    public void flush() {

    }

    public Map<StreamPartition, StreamRouterSpec> getRouteSpecMap() {
        return this.routeSpecMap;
    }

    public Map<StreamPartition, List<StreamRoutePartitioner>> getRoutePartitionerMap() {
        return this.routePartitionerMap;
    }
}
