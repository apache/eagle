/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.eagle.security.traffic;

import org.apache.eagle.common.utils.Tuple2;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class SimpleWindowCounter implements Serializable {

    private int windowSize;

    private Map<Long, Long> counter;
    private Queue<Long> timeQueue;

    public SimpleWindowCounter(int size) {
        this.windowSize = size;
        counter = new ConcurrentHashMap<>(windowSize);
        timeQueue = new PriorityQueue<>();
    }

    public boolean insert(long timestamp, long countVal) {
        boolean success = true;
        if (counter.containsKey(timestamp)) {
            counter.put(timestamp, counter.get(timestamp) + countVal);
        } else {
            if (counter.size() < windowSize) {
                counter.put(timestamp, countVal);
                timeQueue.add(timestamp);
            } else {
                success =false;
            }
        }
        return success;
    }

    public int getSize() {
        return counter.size();
    }

    public boolean isFull() {
        return counter.size() >= windowSize;
    }

    public boolean isEmpty() {
        return counter.isEmpty();
    }

    public synchronized Tuple2<Long, Long> poll() {
        long oldestTimestamp = timeQueue.poll();
        Tuple2<Long, Long> pair = new Tuple2<>(oldestTimestamp, counter.get(oldestTimestamp));
        counter.remove(oldestTimestamp);
        return pair;
    }

    public long peek() {
        return timeQueue.peek();
    }

}
