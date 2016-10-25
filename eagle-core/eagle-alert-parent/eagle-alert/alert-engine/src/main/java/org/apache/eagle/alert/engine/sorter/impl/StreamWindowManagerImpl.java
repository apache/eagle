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
package org.apache.eagle.alert.engine.sorter.impl;

import org.apache.eagle.alert.engine.PartitionedEventCollector;
import org.apache.eagle.alert.engine.model.PartitionedEvent;
import org.apache.eagle.alert.engine.sorter.StreamTimeClock;
import org.apache.eagle.alert.engine.sorter.StreamWindow;
import org.apache.eagle.alert.engine.sorter.StreamWindowManager;
import org.apache.eagle.alert.engine.sorter.StreamWindowRepository;
import org.apache.eagle.alert.utils.DateTimeUtil;
import org.apache.eagle.alert.utils.TimePeriodUtils;

import org.apache.commons.lang3.time.StopWatch;
import org.joda.time.Period;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.*;

public class StreamWindowManagerImpl implements StreamWindowManager, Serializable {
    private static final Logger LOG = LoggerFactory.getLogger(StreamWindowManagerImpl.class);
    private final TreeMap<Long, StreamWindow> windowBuckets;
    private transient PartitionedEventCollector collector;
    private final Period windowPeriod;
    private final long windowMargin;
    @SuppressWarnings("unused")
    private final Comparator<PartitionedEvent> comparator;
    private long rejectTime;

    public StreamWindowManagerImpl(Period windowPeriod, long windowMargin, Comparator<PartitionedEvent> comparator, PartitionedEventCollector collector) {
        this.windowBuckets = new TreeMap<>();
        this.windowPeriod = windowPeriod;
        this.windowMargin = windowMargin;
        this.collector = collector;
        this.comparator = comparator;
    }

    @Override
    public StreamWindow addNewWindow(long initialTime) {
        synchronized (windowBuckets) {
            if (!reject(initialTime)) {
                Long windowStartTime = TimePeriodUtils.formatMillisecondsByPeriod(initialTime, windowPeriod);
                Long windowEndTime = windowStartTime + TimePeriodUtils.getMillisecondsOfPeriod(windowPeriod);
                StreamWindow window = StreamWindowRepository.getSingletonInstance().createWindow(windowStartTime, windowEndTime, windowMargin);
                window.register(collector);
                addWindow(window);
                return window;
            } else {
                throw new IllegalStateException("Failed to create new window, as "
                        + DateTimeUtil.millisecondsToHumanDateWithMilliseconds(initialTime) + " is too late, only allow timestamp after "
                        + DateTimeUtil.millisecondsToHumanDateWithMilliseconds(rejectTime));
            }
        }
    }

    private void addWindow(StreamWindow window) {
        if (!windowBuckets.containsKey(window.startTime())) {
            windowBuckets.put(window.startTime(), window);
        } else {
            throw new IllegalArgumentException("Duplicated " + window.toString());
        }
    }

    @Override
    public void removeWindow(StreamWindow window) {
        synchronized (windowBuckets) {
            windowBuckets.remove(window.startTime());
        }
    }

    @Override
    public boolean hasWindow(StreamWindow window) {
        synchronized (windowBuckets) {
            return windowBuckets.containsKey(window.startTime());
        }
    }

    @Override
    public boolean hasWindowFor(long timestamp) {
        return getWindowFor(timestamp) != null;
    }

    @Override
    public Collection<StreamWindow> getWindows() {
        synchronized (windowBuckets) {
            return windowBuckets.values();
        }
    }

    @Override
    public StreamWindow getWindowFor(long timestamp) {
        synchronized (windowBuckets) {
            for (StreamWindow windowBucket : windowBuckets.values()) {
                if (timestamp >= windowBucket.startTime() && timestamp < windowBucket.endTime()) {
                    return windowBucket;
                }
            }
            return null;
        }
    }

    @Override
    public boolean reject(long timestamp) {
        return timestamp < rejectTime;
    }

    @Override
    public void onTick(StreamTimeClock clock, long globalSystemTime) {
        synchronized (windowBuckets) {
            List<StreamWindow> toRemoved = new ArrayList<>();
            List<StreamWindow> aliveWindow = new ArrayList<>();

            for (StreamWindow windowBucket : windowBuckets.values()) {
                windowBucket.onTick(clock, globalSystemTime);
                if (windowBucket.rejectTime() > rejectTime) {
                    rejectTime = windowBucket.rejectTime();
                }
            }
            for (StreamWindow windowBucket : windowBuckets.values()) {
                if (windowBucket.expired() || windowBucket.endTime() <= rejectTime) {
                    toRemoved.add(windowBucket);
                } else {
                    aliveWindow.add(windowBucket);
                }
            }
            toRemoved.forEach(this::closeAndRemoveWindow);
            if (toRemoved.size() > 0) {
                LOG.info("Windows: {} alive = {}, {} expired = {}", aliveWindow.size(), aliveWindow, toRemoved.size(), toRemoved);
            }
        }
    }

    private void closeAndRemoveWindow(StreamWindow windowBucket) {
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        closeWindow(windowBucket);
        removeWindow(windowBucket);
        stopWatch.stop();
        LOG.info("Removed {} in {} ms", windowBucket, stopWatch.getTime());
    }

    private void closeWindow(StreamWindow windowBucket) {
        windowBucket.close();
    }

    public void close() {
        synchronized (windowBuckets) {
            LOG.debug("Closing");
            StopWatch stopWatch = new StopWatch();
            stopWatch.start();
            int count = 0;
            for (StreamWindow windowBucket : getWindows()) {
                count++;
                closeWindow(windowBucket);
            }
            windowBuckets.clear();
            stopWatch.stop();
            LOG.info("Closed {} windows in {} ms", count, stopWatch.getTime());
        }
    }

    public void updateOutputCollector(PartitionedEventCollector outputCollector) {
        this.collector = outputCollector;
        if (windowBuckets != null && !windowBuckets.isEmpty()) {
            windowBuckets.forEach((windowStartTime, streamWindow) -> streamWindow.register(outputCollector));
        }
    }
}