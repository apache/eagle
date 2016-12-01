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

import org.apache.commons.collections.CollectionUtils;
import org.apache.eagle.alert.coordination.model.RouterSpec;
import org.apache.eagle.alert.coordination.model.StreamRouterSpec;
import org.apache.eagle.alert.engine.StreamSparkContextImpl;
import org.apache.eagle.alert.engine.coordinator.StreamDefinition;
import org.apache.eagle.alert.engine.coordinator.StreamPartition;
import org.apache.eagle.alert.engine.coordinator.StreamSortSpec;
import org.apache.eagle.alert.engine.model.PartitionedEvent;
import org.apache.eagle.alert.engine.router.StreamRoutePartitioner;
import org.apache.eagle.alert.engine.router.StreamRouter;
import org.apache.eagle.alert.engine.router.StreamSortHandler;
import org.apache.eagle.alert.engine.router.impl.SparkStreamRouterBoltOutputCollector;
import org.apache.eagle.alert.engine.router.impl.StreamRouterImpl;
import org.apache.eagle.alert.engine.sorter.StreamTimeClock;
import org.apache.eagle.alert.engine.sorter.StreamTimeClockListener;
import org.apache.eagle.alert.engine.spark.model.RouteState;
import org.apache.eagle.alert.engine.spark.model.WindowState;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

public class StreamRouteBoltFunction implements PairFlatMapFunction<Iterator<Tuple2<Integer, Iterable<PartitionedEvent>>>, Integer, PartitionedEvent> {

    private static final Logger LOG = LoggerFactory.getLogger(StreamRouteBoltFunction.class);
    private static final long serialVersionUID = -7211470889316430372L;
    private AtomicReference<Map<String, StreamDefinition>> sdsRef;
    private AtomicReference<RouterSpec> routerSpecRef;

    // mapping from StreamPartition to StreamSortSpec
    private Map<StreamPartition, StreamSortSpec> cachedSSS = new HashMap<>();
    // mapping from StreamPartition(streamId, groupbyspec) to StreamRouterSpec
    private Map<StreamPartition, StreamRouterSpec> cachedSRS = new HashMap<>();

    private WindowState winstate;
    private RouteState routeState;
    private String routeName;

    public StreamRouteBoltFunction(String routeName, AtomicReference<Map<String, StreamDefinition>> sdsRef, AtomicReference<RouterSpec> routerSpecRef, WindowState winstate, RouteState routeState) {
        this.routeName = routeName;
        this.sdsRef = sdsRef;
        this.winstate = winstate;
        this.routeState = routeState;
        this.routerSpecRef = routerSpecRef;
    }

    @Override
    public Iterator<Tuple2<Integer, PartitionedEvent>> call(Iterator<Tuple2<Integer, Iterable<PartitionedEvent>>> tuple2Iterator) throws Exception {

        if (!tuple2Iterator.hasNext()) {
            return Collections.emptyIterator();
        }

        Map<String, StreamDefinition> sdf;
        RouterSpec spec;
        SparkStreamRouterBoltOutputCollector routeCollector = null;
        StreamRouterImpl router = null;
        Tuple2<Integer, Iterable<PartitionedEvent>> tuple2 = tuple2Iterator.next();
        Iterator<PartitionedEvent> events = tuple2._2.iterator();
        int partitionNum = tuple2._1;
        while (events.hasNext()) {
            if (router == null) {
                sdf = sdsRef.get();
                spec = routerSpecRef.get();

                router = new StreamRouterImpl(routeName);
                Map<StreamPartition, StreamRouterSpec> routeSpecMap = routeState.getRouteSpecMapByPartition(partitionNum);
                Map<StreamPartition, List<StreamRoutePartitioner>> routePartitionerMap = routeState.getRoutePartitionerByPartition(partitionNum);
                cachedSSS = routeState.getCachedSSSMapByPartition(partitionNum);
                cachedSRS = routeState.getCachedSRSMapByPartition(partitionNum);
                routeCollector = new SparkStreamRouterBoltOutputCollector(routeSpecMap, routePartitionerMap);
                Map<String, StreamTimeClock> streamTimeClockMap = winstate.getStreamTimeClockByPartition(partitionNum);
                Map<StreamTimeClockListener, String> streamWindowMap = winstate.getStreamWindowsByPartition(partitionNum);
                Map<StreamPartition, StreamSortHandler> streamSortHandlerMap = winstate.getStreamSortHandlerByPartition(partitionNum);

                router.prepare(new StreamSparkContextImpl(null), routeCollector, streamWindowMap, streamTimeClockMap, streamSortHandlerMap);
                onStreamRouteBoltSpecChange(spec, sdf, router, routeCollector, cachedSSS, cachedSRS);

            }

            PartitionedEvent partitionedEvent = events.next();
            router.nextEvent(partitionedEvent);
        }
        cleanup(router);

        return routeCollector.emitResult().iterator();
    }

    public void cleanup(StreamRouter router) {
        if (router != null) {
            StreamRouterImpl routerImpl = (StreamRouterImpl) router;
            routerImpl.closeClock();
        }
    }

    public void onStreamRouteBoltSpecChange(RouterSpec spec, Map<String, StreamDefinition> sds, StreamRouterImpl router, SparkStreamRouterBoltOutputCollector routeCollector,
                                            final Map<StreamPartition, StreamSortSpec> cachedSSS, final Map<StreamPartition, StreamRouterSpec> cachedSRS) {
        //sanityCheck(spec);

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
        if (LOG.isDebugEnabled()) {
            LOG.debug("added StreamSortSpec " + added);
            LOG.debug("removed StreamSortSpec " + removed);
            LOG.debug("modified StreamSortSpec " + modified);
        }
        router.onStreamSortSpecChange(added, removed, modified);
        // switch cache
        this.cachedSSS = newSSS;

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

        if (LOG.isDebugEnabled()) {
            LOG.debug("added StreamRouterSpec " + addedRouterSpecs);
            LOG.debug("removed StreamRouterSpec " + removedRouterSpecs);
            LOG.debug("modified StreamRouterSpec " + modifiedRouterSpecs);
        }

        routeCollector.onStreamRouterSpecChange(addedRouterSpecs, removedRouterSpecs, modifiedRouterSpecs, sds);
        // switch cache
        this.cachedSRS = newSRS;
    }

}
