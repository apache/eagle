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
import org.apache.eagle.alert.engine.router.StreamRoutePartitioner;
import org.apache.eagle.alert.engine.router.impl.SparkStreamRouterBoltOutputCollector;
import org.apache.eagle.alert.engine.spark.accumulator.MapAccum;

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
    private Accumulator<Map<Integer, Map<StreamPartition, List<StreamRoutePartitioner>>>> routePartitionerAccum;
    private Accumulator<Map<Integer, Map<StreamPartition, StreamRouterSpec>>> routeSpecMapAccum;

    public RouteState(JavaStreamingContext jssc) {
        Accumulator<Map<Integer, Map<StreamPartition, List<StreamRoutePartitioner>>>> routePartitionerAccum = jssc.sparkContext().accumulator(new HashMap<>(), "routePartitionerAccum", new MapAccum());
        Accumulator<Map<Integer, Map<StreamPartition, StreamRouterSpec>>> routeSpecMapAccum = jssc.sparkContext().accumulator(new HashMap<>(), "routeSpecAccum", new MapAccum());
        this.routePartitionerAccum = routePartitionerAccum;
        this.routeSpecMapAccum = routeSpecMapAccum;
    }

    public RouteState(Accumulator<Map<Integer, Map<StreamPartition, List<StreamRoutePartitioner>>>> routePartitionerAccum, Accumulator<Map<Integer, Map<StreamPartition, StreamRouterSpec>>> routeSpecMapAccum) {
        this.routePartitionerAccum = routePartitionerAccum;
        this.routeSpecMapAccum = routeSpecMapAccum;
    }

    public void recover() {
        routeSpecMapRef.set(routeSpecMapAccum.value());
        routePartitionerMapRef.set(routePartitionerAccum.value());
        LOG.info("---------routeSpecMapRef----------" + routeSpecMapRef.get());
        LOG.info("---------routePartitionerMapRef----------" + routePartitionerMapRef.get());
    }

    public Map<StreamPartition, StreamRouterSpec> getRouteSpecMapByPartition(int partitionNum) {
        Map<Integer, Map<StreamPartition, StreamRouterSpec>> partitionTorouteSpecMap = routeSpecMapRef.get();
        LOG.info("---RouteState----partitionTorouteSpecMap----------" + (partitionTorouteSpecMap));
        Map<StreamPartition, StreamRouterSpec> routeSpec = partitionTorouteSpecMap.get(partitionNum);
        if (routeSpec == null) {
            routeSpec = new HashMap<>();
        }
        return routeSpec;
    }

    public Map<StreamPartition, List<StreamRoutePartitioner>> getRoutePartitionerByPartition(int partitionNum) {

        Map<Integer, Map<StreamPartition, List<StreamRoutePartitioner>>> partitionToroutePartitioner = routePartitionerMapRef.get();
        LOG.info("---RouteState----partitionToroutePartitioner----------" + (partitionToroutePartitioner));
        Map<StreamPartition, List<StreamRoutePartitioner>> routePartitioner = partitionToroutePartitioner.get(partitionNum);
        if (routePartitioner == null) {
            routePartitioner = new HashMap<>();
        }
        return routePartitioner;
    }

    public void store(SparkStreamRouterBoltOutputCollector routeCollector, int partitionNum) {

        Map<Integer, Map<StreamPartition, StreamRouterSpec>> newRouteSpecMap = new HashMap<>();
        newRouteSpecMap.put(partitionNum, routeCollector.getRouteSpecMap());
        routeSpecMapAccum.add(newRouteSpecMap);

        Map<Integer, Map<StreamPartition, List<StreamRoutePartitioner>>> newRoutePartitionerMap = new HashMap<>();
        newRoutePartitionerMap.put(partitionNum, routeCollector.getRoutePartitionerMap());
        routePartitionerAccum.add(newRoutePartitionerMap);
    }
}
