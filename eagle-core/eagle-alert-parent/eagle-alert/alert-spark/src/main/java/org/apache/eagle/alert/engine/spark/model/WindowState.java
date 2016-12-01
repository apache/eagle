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

import org.apache.eagle.alert.engine.coordinator.StreamPartition;
import org.apache.eagle.alert.engine.router.StreamSortHandler;
import org.apache.eagle.alert.engine.router.impl.StreamRouterImpl;
import org.apache.eagle.alert.engine.sorter.StreamTimeClock;
import org.apache.eagle.alert.engine.sorter.StreamTimeClockListener;
import org.apache.eagle.alert.engine.spark.accumulator.MapToMapAccum;
import org.apache.spark.Accumulator;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

public class WindowState implements Serializable {

    private AtomicReference<Map<Integer, Map<String, StreamTimeClock>>> streamTimeClockRef = new AtomicReference<>();
    private AtomicReference<Map<Integer, Map<StreamTimeClockListener, String>>> streamWindowRef = new AtomicReference<>();
    private AtomicReference<Map<Integer, Map<StreamPartition, StreamSortHandler>>> streamSortHandlersRef = new AtomicReference<>();
    private Accumulator<Map<Integer, Map<String, StreamTimeClock>>> streamIdClockAccum;
    private Accumulator<Map<Integer, Map<StreamTimeClockListener, String>>> streamWindowAccum;
    private Accumulator<Map<Integer, Map<StreamPartition, StreamSortHandler>>> streamSortHandlersAccum;

    private static final Logger LOG = LoggerFactory.getLogger(WindowState.class);

    public WindowState(JavaStreamingContext jssc) {
        Accumulator<Map<Integer, Map<String, StreamTimeClock>>> streamIdClockAccum = jssc.sparkContext().accumulator(new HashMap<>(), "streamIdClock", new MapToMapAccum());
        Accumulator<Map<Integer, Map<StreamTimeClockListener, String>>> streamWindowAccum = jssc.sparkContext().accumulator(new HashMap<>(), "streamWindow", new MapToMapAccum());
        Accumulator<Map<Integer, Map<StreamPartition, StreamSortHandler>>> streamSortHandlersAccum = jssc.sparkContext().accumulator(new HashMap<>(), "streamSortHandlers", new MapToMapAccum());
        this.streamIdClockAccum = streamIdClockAccum;
        this.streamWindowAccum = streamWindowAccum;
        this.streamSortHandlersAccum = streamSortHandlersAccum;
    }

    public WindowState(Accumulator<Map<Integer, Map<String, StreamTimeClock>>> streamIdClockAccum, Accumulator<Map<Integer, Map<StreamTimeClockListener, String>>> streamWindowAccum,
                       Accumulator<Map<Integer, Map<StreamPartition, StreamSortHandler>>> streamSortHandlersAccum) {
        this.streamIdClockAccum = streamIdClockAccum;
        this.streamWindowAccum = streamWindowAccum;
        this.streamSortHandlersAccum = streamSortHandlersAccum;
    }

    public Map<Integer, Map<String, StreamTimeClock>> getStreamTimeClock() {
        return streamTimeClockRef.get();
    }

    public Map<Integer, Map<StreamTimeClockListener, String>> getStreamWindows() {
        return streamWindowRef.get();
    }

    public void recover() {
        streamTimeClockRef.set(streamIdClockAccum.value());
        streamWindowRef.set(streamWindowAccum.value());
        streamSortHandlersRef.set(streamSortHandlersAccum.value());

        LOG.debug("---------streamTimeClock----------" + streamTimeClockRef.get());
        LOG.debug("---------streamWindowRef----------" + streamWindowRef.get());
        LOG.debug("---------streamSortHandlersRef----------" + streamSortHandlersRef.get());
    }

    public void store(StreamRouterImpl router, int partitionNum) {

        //store clock
        Map<Integer, Map<String, StreamTimeClock>> newStreamTimeClock = new HashMap<>();
        newStreamTimeClock.put(partitionNum, router.getStreamTimeClock());
        streamIdClockAccum.add(newStreamTimeClock);
        //store window
        Map<Integer, Map<StreamTimeClockListener, String>> newStreamWindow = new HashMap<>();
        newStreamWindow.put(partitionNum, router.getAllListenerStreamIdMap());
        streamWindowAccum.add(newStreamWindow);
        //store sort handler
        Map<Integer, Map<StreamPartition, StreamSortHandler>> newstreamSortHandler = new HashMap<>();
        newstreamSortHandler.put(partitionNum, router.getAllStreamSortHandlerMap());
        streamSortHandlersAccum.add(newstreamSortHandler);
    }

    public Map<String, StreamTimeClock> getStreamTimeClockByPartition(int partitionNum) {
        Map<Integer, Map<String, StreamTimeClock>> partitionToStreamClock = getStreamTimeClock();
        LOG.debug("---StreamRouteBoltFunction----getStreamTimeClockByPartition----------" + (partitionToStreamClock));
        Map<String, StreamTimeClock> streamTimeClockMap = partitionToStreamClock.get(partitionNum);
        if (streamTimeClockMap == null) {
            streamTimeClockMap = new HashMap<>();
        }
        return streamTimeClockMap;
    }

    public Map<StreamTimeClockListener, String> getStreamWindowsByPartition(int partitionNum) {

        Map<Integer, Map<StreamTimeClockListener, String>> partitionToStreamWindow = getStreamWindows();
        LOG.debug("---StreamRouteBoltFunction----getStreamWindowsByPartition----------" + (partitionToStreamWindow));
        Map<StreamTimeClockListener, String> streamWindowMap = partitionToStreamWindow.get(partitionNum);
        if (streamWindowMap == null) {
            streamWindowMap = new HashMap<>();
        }
        return streamWindowMap;
    }

    public Map<StreamPartition, StreamSortHandler> getStreamSortHandlerByPartition(int partitionNum) {
        Map<Integer, Map<StreamPartition, StreamSortHandler>> partitionToStreamSortHandler = streamSortHandlersRef.get();
        LOG.debug("---StreamRouteBoltFunction----streamSortHandlerMap----------" + (partitionToStreamSortHandler));
        Map<StreamPartition, StreamSortHandler> streamSortHandlerMap = partitionToStreamSortHandler.get(partitionNum);
        if (streamSortHandlerMap == null) {
            streamSortHandlerMap = new HashMap<>();
        }
        return streamSortHandlerMap;
    }
}
