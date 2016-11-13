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
package org.apache.eagle.alert.engine.sorter;

import org.apache.eagle.alert.engine.PartitionedEventCollector;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.eagle.common.DateTimeUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * TODO: Make sure thread-safe.
 * TODO: Leverage Off-Heap Memory to persist append-only events collection.
 */
public abstract class BaseStreamWindow implements StreamWindow, Serializable {
    private final long endTime;
    private final long startTime;
    private final long margin;
    private final AtomicBoolean expired;
    private final long createdTime;
    private static final Logger LOG = LoggerFactory.getLogger(BaseStreamWindow.class);
    private transient PartitionedEventCollector collector;
    private final AtomicLong lastFlushedStreamTime;
    private final AtomicLong lastFlushedSystemTime;

    public BaseStreamWindow(long startTime, long endTime, long marginTime) {
        if (startTime >= endTime) {
            throw new IllegalArgumentException("startTime: " + startTime + " >= endTime: " + endTime + ", expected: startTime < endTime");
        }
        if (marginTime > endTime - startTime) {
            throw new IllegalArgumentException("marginTime: " + marginTime + " > endTime: " + endTime + " - startTime " + startTime + ", expected: marginTime < endTime - startTime");
        }
        this.startTime = startTime;
        this.endTime = endTime;
        this.margin = marginTime;
        this.expired = new AtomicBoolean(false);
        this.createdTime = System.currentTimeMillis();
        this.lastFlushedStreamTime = new AtomicLong(0);
        this.lastFlushedSystemTime = new AtomicLong(this.createdTime);
    }

    @Override
    public void register(PartitionedEventCollector collector) {
        if (this.collector != null) {
            throw new IllegalArgumentException("Duplicated collector error");
        }
        this.collector = collector;
    }

    @Override
    public long createdTime() {
        return createdTime;
    }

    public long startTime() {
        return this.startTime;
    }

    @Override
    public long rejectTime() {
        return this.lastFlushedStreamTime.get();
    }

    @Override
    public long margin() {
        return this.margin;
    }

    public long endTime() {
        return this.endTime;
    }

    public boolean accept(final long eventTime) {
        return !expired() && eventTime >= startTime && eventTime < endTime
                && eventTime >= lastFlushedStreamTime.get(); // dropped
    }

    public boolean expired() {
        return expired.get();
    }

    @Override
    public boolean alive() {
        return !expired.get();
    }

    /**
     * Expire when
     * 1) If stream time >= endTime + marginTime, then flush and expire
     * 2) If systemTime - flushedTime > endTime - startTime + marginTime && streamTime >= endTime, then flush and expire.
     * 3) If systemTime - flushedTime > endTime - startTime + marginTime && streamTime < endTime, then flush but not expire.
     * 4) else do nothing
     *
     * @param clock            stream time clock
     * @param globalSystemTime system time clock
     */
    @Override
    public synchronized void onTick(StreamTimeClock clock, long globalSystemTime) {
        if (!expired()) {
            if (clock.getTime() >= endTime + margin) {
                LOG.info("Expiring {} at stream time:{}, latency:{}, window: {}", clock.getStreamId(),
                        DateTimeUtil.millisecondsToHumanDateWithMilliseconds(clock.getTime()), globalSystemTime - lastFlushedSystemTime.get(), this);
                lastFlushedStreamTime.set(clock.getTime());
                lastFlushedSystemTime.set(globalSystemTime);
                flush();
                expired.set(true);
            } else if (globalSystemTime - lastFlushedSystemTime.get() >= endTime + margin - startTime && size() > 0) {
                LOG.info("Flushing {} at system time: {}, stream time: {}, latency: {}, window: {}", clock.getStreamId(),
                        DateTimeUtil.millisecondsToHumanDateWithMilliseconds(globalSystemTime),
                        DateTimeUtil.millisecondsToHumanDateWithMilliseconds(clock.getTime()), globalSystemTime - lastFlushedSystemTime.get(), this);
                lastFlushedStreamTime.set(clock.getTime());
                lastFlushedSystemTime.set(globalSystemTime);
                flush();
                if (lastFlushedStreamTime.get() >= this.endTime) {
                    expired.set(true);
                }
            }
        } else {
            LOG.warn("Window has already expired, should not tick: {}", this.toString());
        }
    }

    public void close() {
        flush();
        expired.set(true);
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder().append(startTime).append(endTime).append(margin).build();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj != null && obj instanceof BaseStreamWindow) {
            BaseStreamWindow another = (BaseStreamWindow) obj;
            return another.startTime == this.startTime && another.endTime == this.endTime && another.margin == this.margin;
        }
        return false;
    }

    @Override
    public void flush() {
        if (this.collector == null) {
            throw new NullPointerException("Collector is not given before window flush");
        }
        this.flush(collector);
    }

    /**
     * @param collector PartitionedEventCollector.
     * @return max timestamp.
     */
    protected abstract void flush(PartitionedEventCollector collector);

    @Override
    public String toString() {
        return String.format("StreamWindow[period=[%s,%s), margin=%s ms, size=%s, reject=%s]",
                DateTimeUtil.millisecondsToHumanDateWithMilliseconds(this.startTime),
                DateTimeUtil.millisecondsToHumanDateWithMilliseconds(this.endTime),
                this.margin,
                size(),
                this.rejectTime() == 0 ? DateTimeUtil.millisecondsToHumanDateWithMilliseconds(this.startTime) : DateTimeUtil.millisecondsToHumanDateWithMilliseconds(this.rejectTime())
        );
    }
}