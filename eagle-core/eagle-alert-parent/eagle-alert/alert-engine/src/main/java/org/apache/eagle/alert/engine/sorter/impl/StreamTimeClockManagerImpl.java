/*
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
package org.apache.eagle.alert.engine.sorter.impl;

import org.apache.eagle.alert.engine.sorter.StreamTimeClock;
import org.apache.eagle.alert.engine.sorter.StreamTimeClockListener;
import org.apache.eagle.alert.engine.sorter.StreamTimeClockManager;
import org.apache.eagle.common.DateTimeUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicInteger;

public final class StreamTimeClockManagerImpl implements StreamTimeClockManager {
    private static final long serialVersionUID = -2770823821511195343L;
    private static final Logger LOG = LoggerFactory.getLogger(StreamTimeClockManagerImpl.class);
    private final Map<String, StreamTimeClock> streamIdTimeClockMap;
    private Timer timer;

    private final Map<StreamTimeClockListener, String> listenerStreamIdMap;
    private static final AtomicInteger num = new AtomicInteger();

    public StreamTimeClockManagerImpl() {
        this(new HashMap<>(), new HashMap<>());
    }

    public StreamTimeClockManagerImpl(Map<StreamTimeClockListener, String> listenerStreamIdMap, Map<String, StreamTimeClock> streamIdTimeClockMap) {

        this.listenerStreamIdMap = listenerStreamIdMap;
        this.streamIdTimeClockMap = streamIdTimeClockMap;
        timer = new Timer("StreamScheduler-" + num.getAndIncrement());
        timer.schedule(new TimerTask() {
            @Override
            public void run() {
                // Make sure the timer tick happens one by one
                triggerTickOnAll();
            }
        }, 1000, 1000);
    }

    /**
     * By default, we could keep the current time clock in memory,
     * Eventually we may need to consider the global time synchronization across all nodes
     * 1) When to initialize window according to start time
     * 2) When to close expired window according to current time
     *
     * @return StreamTimeClock instance.
     */
    @Override
    public StreamTimeClock createStreamTimeClock(String streamId) {
        synchronized (streamIdTimeClockMap) {
            if (!streamIdTimeClockMap.containsKey(streamId)) {
                StreamTimeClock instance = new StreamTimeClockInLocalMemory(streamId);
                LOG.info("Created {}", instance);
                streamIdTimeClockMap.put(streamId, instance);
            } else {
                LOG.warn("TimeClock for stream already existss: " + streamIdTimeClockMap.get(streamId));
            }
            return streamIdTimeClockMap.get(streamId);
        }
    }

    @Override
    public StreamTimeClock getStreamTimeClock(String streamId) {
        synchronized (streamIdTimeClockMap) {
            if (!streamIdTimeClockMap.containsKey(streamId)) {
                LOG.warn("TimeClock for stream {} is not initialized before being called, create now", streamId);
                return createStreamTimeClock(streamId);
            }
            return streamIdTimeClockMap.get(streamId);
        }
    }

    @Override
    public void removeStreamTimeClock(String streamId) {
        synchronized (streamIdTimeClockMap) {
            if (streamIdTimeClockMap.containsKey(streamId)) {
                streamIdTimeClockMap.remove(streamId);
                LOG.info("Removed TimeClock for stream {}: {}", streamId, streamIdTimeClockMap.get(streamId));
            } else {
                LOG.warn("No TimeClock found for stream {}, nothing to remove", streamId);
            }
        }
    }

    @Override
    public Map<String, StreamTimeClock> getAllStreamTimeClock() {
        return this.streamIdTimeClockMap;
    }

    @Override
    public Map<StreamTimeClockListener, String> getAllListenerStreamIdMap() {
        return this.listenerStreamIdMap;
    }

    @Override
    public void registerListener(String streamId, StreamTimeClockListener listener) {
        synchronized (listenerStreamIdMap) {
            if (listenerStreamIdMap.containsKey(listener)) {
                throw new IllegalArgumentException("Duplicated listener: " + listener.toString());
            }
            LOG.info("Register {} on {}", listener, streamId);
            listenerStreamIdMap.put(listener, streamId);
        }
    }

    @Override
    public void registerListener(StreamTimeClock streamClock, StreamTimeClockListener listener) {
        registerListener(streamClock.getStreamId(), listener);
    }

    @Override
    public void removeListener(StreamTimeClockListener listener) {
        listenerStreamIdMap.remove(listener);
    }

    @Override
    public synchronized void triggerTickOn(String streamId) {
        int count = 0;
        for (Map.Entry<StreamTimeClockListener, String> entry : listenerStreamIdMap.entrySet()) {
            if (entry.getValue().equals(streamId)) {
                entry.getKey().onTick(streamIdTimeClockMap.get(streamId), getCurrentSystemTime());
                count++;
            }
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("Triggered {} time-clock listeners on stream {}", count, streamId);
        }
    }

    private static long getCurrentSystemTime() {
        return System.currentTimeMillis();
    }

    @Override
    public void onTimeUpdate(String streamId, long timestamp) {
        StreamTimeClock timeClock = getStreamTimeClock(streamId);
        if (timeClock == null) {
            return;
        }
        // Trigger time clock only when time moves forward
        if (timestamp >= timeClock.getTime()) {
            timeClock.moveForward(timestamp);
            if (LOG.isDebugEnabled()) {
                LOG.debug("Tick on stream {} with latest time {}", streamId, DateTimeUtil.millisecondsToHumanDateWithMilliseconds(timeClock.getTime()));
            }
            triggerTickOn(streamId);
        }
    }

    private void triggerTickOnAll() {
        synchronized (listenerStreamIdMap) {
            for (Map.Entry<StreamTimeClockListener, String> entry : listenerStreamIdMap.entrySet()) {
                triggerTickOn(entry.getValue());
            }
        }
    }

    @Override
    public void close() {
        timer.cancel();
        triggerTickOnAll();
        LOG.info("Closed StreamTimeClockManager {}", this);
    }
}