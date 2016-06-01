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


/**
 * Possible implementation:
 *
 * 1) EventTimeClockTrigger (by default)
 * 2) SystemTimeClockTrigger
 */
public interface StreamTimeClockTrigger {
    /**
     * @param streamId stream id to listen to
     * @param listener to watch on streamId
     */
    void registerListener(String streamId, StreamTimeClockListener listener);

    /**
     *
     * @param streamClock
     * @param listener
     */
    void registerListener(StreamTimeClock streamClock, StreamTimeClockListener listener);

    /**
     * @param listener listener to remove
     */
    void removeListener(StreamTimeClockListener listener);

    /**
     * Trigger tick of all listeners on certain stream
     *
     * @param streamId stream id
     */
    void triggerTickOn(String streamId);

    /**
     * Update time per new event time on stream
     *
     * @param streamId
     * @param timestamp
     */
    void onTimeUpdate(String streamId, long timestamp);

    void close();
}