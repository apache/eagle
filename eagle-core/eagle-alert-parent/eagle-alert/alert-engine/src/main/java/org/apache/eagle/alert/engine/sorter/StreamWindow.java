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
import org.apache.eagle.alert.engine.model.PartitionedEvent;

/**
 * <h2>Tumbling Window instead Sliding Window</h2>
 * We could have time overlap to sort out-of-ordered stream,
 * but each window should never have events overlap, otherwise will have logic problem.
 * <p>
 * <p>
 * <h2>Ingestion Time Policy</h2>
 * Different notions of time, namely processing time, event time, and ingestion time.
 * <p>
 * <ol>
 * <li>
 * In processing time, windows are defined with respect to the wall clock of the machine that builds and processes a window, i.e., a one minute processing time window collects elements for exactly one minute.
 * </li>
 * <li>
 * In event time, windows are defined with respect to timestamps that are attached to each event record. This is common for many types of events, such as log entries, sensor data, etc, where the timestamp usually represents the time at which the event occurred. Event time has several benefits over processing time. First of all, it decouples the program semantics from the actual serving speed of the source and the processing performance of system. Hence you can process historic data, which is served at maximum speed, and continuously produced data with the same program. It also prevents semantically incorrect results in case of backpressure or delays due to failure recovery. Second, event time windows compute correct results, even if events arrive out-of-order of their timestamp which is common if a data stream gathers events from distributed sources.
 * </li>
 * <li>
 * Ingestion time is a hybrid of processing and event time. It assigns wall clock timestamps to records as soon as they arrive in the system (at the source) and continues processing with event time semantics based on the attached timestamps.
 * </li>
 * </ol>
 */
public interface StreamWindow extends StreamTimeClockListener {
    /**
     * @return Created timestamp
     */
    long createdTime();

    /**
     * Get start time
     *
     * @return
     */
    long startTime();

    long margin();

    /**
     * @return reject timestamp < rejectTime()
     */
    long rejectTime();

    /**
     * Get end time
     *
     * @return
     */
    long endTime();

    /**
     * @param timestamp event time
     * @return true/false in boolean
     */
    boolean accept(long timestamp);

    /**
     * Window is expired
     *
     * @return whether window is expired
     */
    boolean expired();

    /**
     * @return whether window is alive
     */
    boolean alive();

    /**
     * @param event
     */
    boolean add(PartitionedEvent event);

//    /**
//     * @param collector Drain to output collector
//     */
//    void flush(PartitionedEventCollector collector);

    void flush();

    /**
     * Close window
     */
    void close();

    void register(PartitionedEventCollector collector);

    /**
     * @return
     */
    int size();
}