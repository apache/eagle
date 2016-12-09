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

import org.apache.eagle.alert.engine.sorter.StreamTimeClock;
import org.apache.eagle.common.DateTimeUtil;

import java.util.concurrent.atomic.AtomicLong;


/**
 * In memory thread-safe time clock service.
 * TODO: maybe need to synchronize time clock globally, how to?
 */
public class StreamTimeClockInLocalMemory implements StreamTimeClock {
    private static final long serialVersionUID = 4426801755375289391L;
    private final AtomicLong currentTime;
    private final String streamId;

    public StreamTimeClockInLocalMemory(String streamId, long initialTime) {
        this.streamId = streamId;
        this.currentTime = new AtomicLong(initialTime);
    }

    public StreamTimeClockInLocalMemory(String streamId) {
        this(streamId, 0L);
    }

    @Override
    public void moveForward(long timestamp) {
        if (timestamp < currentTime.get()) {
            throw new IllegalArgumentException(timestamp + " < " + currentTime.get() + ", should not move time back");
        }
        this.currentTime.set(timestamp);
    }

    @Override
    public String getStreamId() {
        return streamId;
    }

    @Override
    public long getTime() {
        return currentTime.get();
    }

    @Override
    public String toString() {
        return String.format("StreamClock[streamId=%s, now=%s]", streamId, DateTimeUtil.millisecondsToHumanDateWithMilliseconds(currentTime.get()));
    }
}