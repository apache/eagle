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

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.eagle.alert.engine.PartitionedEventCollector;
import org.apache.eagle.alert.engine.StreamContext;
import org.apache.eagle.alert.engine.coordinator.StreamPartition;
import org.apache.eagle.alert.engine.coordinator.StreamSortSpec;
import org.apache.eagle.alert.engine.model.PartitionedEvent;
import org.apache.eagle.alert.engine.router.StreamRouter;
import org.apache.eagle.alert.engine.router.StreamSortHandler;
import org.apache.eagle.alert.engine.sorter.StreamTimeClock;
import org.apache.eagle.alert.engine.sorter.StreamTimeClockListener;
import org.apache.eagle.alert.engine.sorter.StreamTimeClockManager;
import org.apache.eagle.alert.engine.sorter.impl.StreamSortWindowHandlerImpl;
import org.apache.eagle.alert.engine.sorter.impl.StreamTimeClockManagerImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StreamRouterImpl implements StreamRouter {
    private static final long serialVersionUID = -4640125063690900014L;
    private final static Logger LOG = LoggerFactory.getLogger(StreamRouterImpl.class);
    private final String name;
    private volatile Map<StreamPartition,StreamSortHandler> streamSortHandlers;
    private PartitionedEventCollector outputCollector;
    private StreamTimeClockManager streamTimeClockManager;
    private StreamContext context;

    /**
     * @param name This name should be formed by topologyId + router id, which is built by topology builder
     */
    public StreamRouterImpl(String name){
        this.name = name;
    }

    public String getName(){
        return this.name;
    }

    @Override
    public void close() {
        streamSortHandlers.values().forEach(StreamSortHandler::close);
        streamTimeClockManager.close();
    }

    public void prepare(StreamContext context, PartitionedEventCollector outputCollector) {
        this.streamTimeClockManager = new StreamTimeClockManagerImpl();
        this.streamSortHandlers = new HashMap<>();
        this.outputCollector = outputCollector;
        this.context = context;
    }

    public void prepare(StreamContext context, PartitionedEventCollector outputCollector, Map<StreamTimeClockListener, String> listenerStreamIdMap, Map<String, StreamTimeClock> streamIdTimeClockMap, Map<StreamPartition, StreamSortHandler> streamSortHandlerMap) {

        this.streamSortHandlers = streamSortHandlerMap;
        this.outputCollector = outputCollector;
        listenerStreamIdMap.forEach((k,v)->{
            StreamSortWindowHandlerImpl streamwindow = (StreamSortWindowHandlerImpl)k;
            streamwindow.updateOutputCollector(this.outputCollector);
        });
        this.streamTimeClockManager = new StreamTimeClockManagerImpl(listenerStreamIdMap, streamIdTimeClockMap);
        this.context = context;
    }

    /**
     * TODO: Potential improvement: if StreamSortHandler is expensive, we can use DISRUPTOR to buffer
     *
     * @param event StreamEvent
     */
    public void nextEvent(PartitionedEvent event) {
        this.context.counter().scope("receive_count").incr();
        if(!dispatchToSortHandler(event)) {
            this.context.counter().scope("direct_count").incr();
            // Pass through directly if no need to sort
            outputCollector.emit(event);
        }
        this.context.counter().scope("sort_count").incr();
        // Update stream clock time if moving forward and trigger all tick listeners
        streamTimeClockManager.onTimeUpdate(event.getStreamId(),event.getTimestamp());
    }

    /**
     * @param event input event
     * @return whether sorted
     */
    private boolean dispatchToSortHandler(PartitionedEvent event){
        if(event.getTimestamp() <= 0) return false;

        StreamSortHandler sortHandler = streamSortHandlers.get(event.getPartition());
        if(sortHandler == null){
            if(event.isSortRequired()) {
                LOG.warn("Stream sort handler required has not been loaded so emmit directly: {}", event);
                this.context.counter().scope("miss_sort_count").incr();
            }
            return false;
        } else {
            sortHandler.nextEvent(event);
            return true;
        }
    }

    @Override
    public void onStreamSortSpecChange(Map<StreamPartition, StreamSortSpec> added,
            Map<StreamPartition, StreamSortSpec> removed,
            Map<StreamPartition, StreamSortSpec> changed) {
        synchronized (streamTimeClockManager) {
            Map<StreamPartition, StreamSortHandler> copy = new HashMap<>(this.streamSortHandlers);
            // add new StreamSortSpec
            if (added != null && added.size() > 0) {
                for (Entry<StreamPartition, StreamSortSpec> spec : added.entrySet()) {
                    StreamPartition tmp = spec.getKey();
                    if (copy.containsKey(tmp)) {
                        LOG.error("Metadata calculation error: Duplicated StreamSortSpec " + spec);
                    } else {
                        StreamSortHandler handler = new StreamSortWindowHandlerImpl();
                        handler.prepare(tmp.getStreamId(),spec.getValue(), this.outputCollector);
                        copy.put(tmp, handler);
                        streamTimeClockManager.registerListener(streamTimeClockManager.createStreamTimeClock(tmp.getStreamId()), handler);
                    }
                }
            }

            // remove StreamSortSpec
            if (removed != null && removed.size() > 0) {
                for (Entry<StreamPartition, StreamSortSpec> spec : removed.entrySet()) {
                    StreamPartition tmp = spec.getKey();
                    if (copy.containsKey(tmp)) {
                        copy.get(tmp).close();
                        streamTimeClockManager.removeListener(copy.get(tmp));
                        copy.remove(tmp);
                    } else {
                        LOG.error("Metadata calculation error: remove nonexisting StreamSortSpec " + spec.getValue());
                    }
                }
            }

            // modify StreamSortSpec
            if (changed != null && changed.size() > 0) {
                for (Entry<StreamPartition, StreamSortSpec> spec : changed.entrySet()) {
                    StreamPartition tmp = spec.getKey();
                    if (copy.containsKey(tmp)) {
                        copy.get(tmp).close();
                        streamTimeClockManager.removeListener(copy.get(tmp));
                        copy.remove(tmp);
                        StreamSortHandler handler = new StreamSortWindowHandlerImpl();
                        handler.prepare(tmp.getStreamId(), spec.getValue(), this.outputCollector);
                        copy.put(tmp, handler);
                        streamTimeClockManager.registerListener(tmp.getStreamId(), handler);
                    } else {
                        LOG.error("Metadata calculation error: modify non-existing StreamSortSpec " + spec.getValue());
                    }
                }
            }
            // atomic switch
            this.streamSortHandlers = copy;
        }
    }

    public Map<String, StreamTimeClock> getStreamTimeClock() {
        return streamTimeClockManager.getAllStreamTimeClock();
    }

    public Map<StreamTimeClockListener, String> getAllListenerStreamIdMap() {
        return streamTimeClockManager.getAllListenerStreamIdMap();
    }
    public Map<StreamPartition,StreamSortHandler> getAllStreamSortHandlerMap() {
        return this.streamSortHandlers;
    }
}