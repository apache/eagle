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

package org.apache.eagle.alert.engine.spark.model;

import org.apache.eagle.alert.coordination.model.StreamRouterSpec;
import org.apache.eagle.alert.engine.coordinator.StreamPartition;
import org.apache.eagle.alert.engine.coordinator.StreamSortSpec;
import org.apache.eagle.alert.engine.router.StreamRoutePartitioner;
import org.apache.eagle.alert.engine.router.impl.SparkStreamRouterBoltOutputCollector;
import org.apache.eagle.alert.engine.spark.accumulator.MapToMapAccum;
import org.apache.spark.Accumulator;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

public class RouteState implements Serializable {

    private static final Logger LOG = LoggerFactory.getLogger(RouteState.class);
    private AtomicReference<Map<Integer, Map<StreamPartition, StreamRouterSpec>>> routeSpecMapRef = new AtomicReference<>();
    private AtomicReference<Map<Integer, Map<StreamPartition, List<StreamRoutePartitioner>>>> routePartitionerMapRef = new AtomicReference<>();
    private AtomicReference<Map<Integer, Map<StreamPartition, StreamSortSpec>>> cachedSSSRef = new AtomicReference<>();
    private AtomicReference<Map<Integer, Map<StreamPartition, StreamRouterSpec>>> cachedSRSRef = new AtomicReference<>();

    private Accumulator<Map<Integer, Map<StreamPartition, StreamSortSpec>>> cachedSSSAccm;
    private Accumulator<Map<Integer, Map<StreamPartition, StreamRouterSpec>>> cachedSRSAccm;
    private Accumulator<Map<Integer, Map<StreamPartition, List<StreamRoutePartitioner>>>> routePartitionerAccum;
    private Accumulator<Map<Integer, Map<StreamPartition, StreamRouterSpec>>> routeSpecMapAccum;

    public RouteState(JavaStreamingContext jssc) {
        Accumulator<Map<Integer, Map<StreamPartition, List<StreamRoutePartitioner>>>> routePartitionerAccum = jssc.sparkContext().accumulator(new HashMap<>(), "routePartitionerAccum", new MapToMapAccum());
        this.routePartitionerAccum = routePartitionerAccum;
        Accumulator<Map<Integer, Map<StreamPartition, StreamRouterSpec>>> routeSpecMapAccum = jssc.sparkContext().accumulator(new HashMap<>(), "routeSpecAccum", new MapToMapAccum());
        this.routeSpecMapAccum = routeSpecMapAccum;
        Accumulator<Map<Integer, Map<StreamPartition, StreamSortSpec>>> cachedSSSAccm = jssc.sparkContext().accumulator(new HashMap<>(), "cachedSSSAccm", new MapToMapAccum());
        this.cachedSSSAccm = cachedSSSAccm;
        Accumulator<Map<Integer, Map<StreamPartition, StreamRouterSpec>>> cachedSRSAccm = jssc.sparkContext().accumulator(new HashMap<>(), "cachedSRSAccm", new MapToMapAccum());
        this.cachedSRSAccm = cachedSRSAccm;

    }

    public RouteState(Accumulator<Map<Integer, Map<StreamPartition, List<StreamRoutePartitioner>>>> routePartitionerAccum,
                      Accumulator<Map<Integer, Map<StreamPartition, StreamRouterSpec>>> routeSpecMapAccum) {
        this.routePartitionerAccum = routePartitionerAccum;
        this.routeSpecMapAccum = routeSpecMapAccum;
    }

    public void recover() {
        routeSpecMapRef.set(routeSpecMapAccum.value());
        routePartitionerMapRef.set(routePartitionerAccum.value());
        cachedSSSRef.set(cachedSSSAccm.value());
        cachedSRSRef.set(cachedSRSAccm.value());
        LOG.debug("---------routeSpecMapRef----------" + routeSpecMapRef.get());
        LOG.debug("---------routePartitionerMapRef----------" + routePartitionerMapRef.get());
        LOG.debug("---------cachedSSSRef----------" + cachedSSSRef.get());
        LOG.debug("---------cachedSRSRef----------" + cachedSRSRef.get());
    }

    public Map<StreamPartition, StreamRouterSpec> getRouteSpecMapByPartition(int partitionNum) {
        Map<Integer, Map<StreamPartition, StreamRouterSpec>> partitionTorouteSpecMap = routeSpecMapRef.get();
        LOG.debug("---RouteState----getRouteSpecMapByPartition----------" + (partitionTorouteSpecMap));
        Map<StreamPartition, StreamRouterSpec> routeSpec = partitionTorouteSpecMap.get(partitionNum);
        if (routeSpec == null) {
            routeSpec = new HashMap<>();
        }
        return routeSpec;
    }

    public Map<StreamPartition, List<StreamRoutePartitioner>> getRoutePartitionerByPartition(int partitionNum) {

        Map<Integer, Map<StreamPartition, List<StreamRoutePartitioner>>> partitionToroutePartitioner = routePartitionerMapRef.get();
        LOG.debug("---RouteState----getRoutePartitionerByPartition----------" + (partitionToroutePartitioner));
        Map<StreamPartition, List<StreamRoutePartitioner>> routePartitioner = partitionToroutePartitioner.get(partitionNum);
        if (routePartitioner == null) {
            routePartitioner = new HashMap<>();
        }
        return routePartitioner;
    }

    public Map<StreamPartition, StreamSortSpec> getCachedSSSMapByPartition(int partitionNum) {
        Map<Integer, Map<StreamPartition, StreamSortSpec>> cachedSSSMap =  cachedSSSRef.get();
        LOG.debug("---RouteState----getCachedSSSMapByPartition----------" + (cachedSSSMap));
        Map<StreamPartition, StreamSortSpec> cachedSSS = cachedSSSMap.get(partitionNum);
        if (cachedSSS == null) {
            cachedSSS = new HashMap<>();
        }
        return cachedSSS;
    }

    public Map<StreamPartition, StreamRouterSpec> getCachedSRSMapByPartition(int partitionNum) {
        Map<Integer, Map<StreamPartition, StreamRouterSpec>> cachedSRSMap =  cachedSRSRef.get();
        LOG.debug("---RouteState----getCachedSRSMapByPartition----------" + (cachedSRSMap));
        Map<StreamPartition, StreamRouterSpec> cachedSRS = cachedSRSMap.get(partitionNum);
        if (cachedSRS == null) {
            cachedSRS = new HashMap<>();
        }
        return cachedSRS;
    }

    public void store(SparkStreamRouterBoltOutputCollector routeCollector, Map<StreamPartition, StreamSortSpec> cachedSSS, Map<StreamPartition, StreamRouterSpec> cachedSRS, int partitionNum) {

        Map<Integer, Map<StreamPartition, StreamRouterSpec>> newRouteSpecMap = new HashMap<>();
        newRouteSpecMap.put(partitionNum, routeCollector.getRouteSpecMap());
        routeSpecMapAccum.add(newRouteSpecMap);

        Map<Integer, Map<StreamPartition, List<StreamRoutePartitioner>>> newRoutePartitionerMap = new HashMap<>();
        newRoutePartitionerMap.put(partitionNum, routeCollector.getRoutePartitionerMap());
        routePartitionerAccum.add(newRoutePartitionerMap);


        Map<Integer, Map<StreamPartition, StreamSortSpec>> newCachedSSSMap = new HashMap<>();
        newCachedSSSMap.put(partitionNum, cachedSSS);
        cachedSSSAccm.add(newCachedSSSMap);

        Map<Integer, Map<StreamPartition, StreamRouterSpec>> newCachedSRSMap = new HashMap<>();
        newCachedSRSMap.put(partitionNum, cachedSRS);
        cachedSRSAccm.add(newCachedSRSMap);
    }
}
