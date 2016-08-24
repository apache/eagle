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

import org.apache.eagle.alert.coordination.model.StreamRouterSpec;
import org.apache.eagle.alert.engine.StreamContextImpl;
import org.apache.eagle.alert.engine.coordinator.StreamDefinition;
import org.apache.eagle.alert.engine.coordinator.StreamPartition;
import org.apache.eagle.alert.engine.coordinator.StreamSortSpec;
import org.apache.eagle.alert.engine.model.PartitionedEvent;
import org.apache.eagle.alert.engine.router.StreamRoutePartitioner;
import org.apache.eagle.alert.engine.router.StreamRouter;
import org.apache.eagle.alert.engine.router.StreamSortHandler;
import org.apache.eagle.alert.engine.router.impl.SparkStreamRouterBoltOutputCollector;
import org.apache.eagle.alert.engine.router.impl.StreamRouterImpl;

import backtype.storm.metric.api.MultiCountMetric;
import org.apache.eagle.alert.engine.sorter.StreamTimeClock;
import org.apache.eagle.alert.engine.sorter.StreamTimeClockListener;
import org.apache.eagle.alert.engine.spark.model.RouteState;
import org.apache.eagle.alert.engine.spark.model.WindowState;
import org.apache.eagle.alert.engine.utils.Constants;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

public class StreamRouteBoltFunction implements PairFlatMapFunction<Iterator<Tuple2<Integer, PartitionedEvent>>, Integer, PartitionedEvent> {

    private static final Logger LOG = LoggerFactory.getLogger(StreamRouteBoltFunction.class);
    private static final long serialVersionUID = -7211470889316430372L;
    private AtomicReference<Map<String, StreamDefinition>> sdsRef;
    private AtomicReference<Map<String, Map<StreamPartition, StreamSortSpec>>> sssChangInfoRef;
    private AtomicReference<Map<String, Collection<StreamRouterSpec>>> srsChangInfoRef;

    private WindowState winstate;
    private RouteState routeState;
    private String routeName;

    public StreamRouteBoltFunction(String routeName, AtomicReference<Map<String, StreamDefinition>> sdsRef, AtomicReference<Map<String, Map<StreamPartition, StreamSortSpec>>> sssChangInfoRef,
                                   AtomicReference<Map<String, Collection<StreamRouterSpec>>> srsChangInfoRef, WindowState winstate, RouteState routeState) {
        this.routeName = routeName;
        this.sdsRef = sdsRef;
        this.sssChangInfoRef = sssChangInfoRef;
        this.srsChangInfoRef = srsChangInfoRef;
        this.winstate = winstate;
        this.routeState = routeState;
    }

    @Override
    public Iterator<Tuple2<Integer, PartitionedEvent>> call(Iterator<Tuple2<Integer, PartitionedEvent>> tuple2Iterator) throws Exception {


        Map<String, StreamDefinition> sdf;
        SparkStreamRouterBoltOutputCollector routeCollector = null;
        StreamRouterImpl router = null;
        int partitionNum = Constants.UNKNOW_PARTITION;

        while (tuple2Iterator.hasNext()) {

            Tuple2<Integer, PartitionedEvent> tuple2 = tuple2Iterator.next();
            if (partitionNum == Constants.UNKNOW_PARTITION) {
                partitionNum = tuple2._1;
            }

            if (router == null) {
                sdf =  sdsRef.get();
                router = new StreamRouterImpl(routeName);
                Map<StreamPartition, StreamRouterSpec> routeSpecMap = routeState.getRouteSpecMapByPartition(partitionNum);
                Map<StreamPartition, List<StreamRoutePartitioner>> routePartitionerMap = routeState.getRoutePartitionerByPartition(partitionNum);
                routeCollector = new SparkStreamRouterBoltOutputCollector(routeSpecMap, routePartitionerMap);
                Map<String, StreamTimeClock> streamTimeClockMap = winstate.getStreamTimeClockByPartition(partitionNum);
                Map<StreamTimeClockListener, String> streamWindowMap = winstate.getStreamWindowsByPartition(partitionNum);
                Map<StreamPartition, StreamSortHandler> streamSortHandlerMap = winstate.getStreamSortHandlerByPartition(partitionNum);

                router.prepare(new StreamContextImpl(null, new MultiCountMetric(), null), routeCollector, streamWindowMap, streamTimeClockMap, streamSortHandlerMap);

                Map<String, Map<StreamPartition, StreamSortSpec>> sssChangeInfo = sssChangInfoRef.get();
                router.onStreamSortSpecChange(sssChangeInfo.get(Constants.ADDED), sssChangeInfo.get(Constants.REMOVED), sssChangeInfo.get(Constants.MODIFIED));

                Map<String, Collection<StreamRouterSpec>> srsChangInfo = srsChangInfoRef.get();
                routeCollector.onStreamRouterSpecChange(srsChangInfo.get(Constants.ADDED), srsChangInfo.get(Constants.REMOVED), srsChangInfo.get(Constants.MODIFIED), sdf);
            }

            PartitionedEvent partitionedEvent = tuple2._2;
            router.nextEvent(partitionedEvent);
        }
        cleanup(router);
        if (partitionNum != Constants.UNKNOW_PARTITION) {
            winstate.store(router, partitionNum);
            routeState.store(routeCollector, partitionNum);
        }
        if(routeCollector == null){
            return Collections.emptyIterator();
        }
        return routeCollector.emitResult().iterator();
    }

    public void cleanup(StreamRouter router) {
        if (router != null) {
            router.close();
        }
    }
}
