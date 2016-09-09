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
package org.apache.eagle.alert.engine.evaluator.nodata;

import org.apache.commons.lang.builder.HashCodeBuilder;

import java.util.*;

/**
 * Since 6/28/16.
 * to get distinct values within a specified time window
 * valueMaxTimeMap : each distinct value is associated with max timestamp it ever had
 * timeSortedMap : map sorted by timestamp first and then value
 * With the above 2 data structure, we can get distinct values in LOG(N).
 */
public class DistinctValuesInTimeWindow {
    public static class ValueAndTime {
        Object value;
        long timestamp;

        public ValueAndTime(Object value, long timestamp) {
            this.value = value;
            this.timestamp = timestamp;
        }

        public String toString() {
            return "[" + value + "," + timestamp + "]";
        }

        public int hashCode() {
            return new HashCodeBuilder().append(value).append(timestamp).toHashCode();
        }

        public boolean equals(Object that) {
            if (!(that instanceof ValueAndTime)) {
                return false;
            }
            ValueAndTime another = (ValueAndTime) that;
            return another.timestamp == this.timestamp && another.value.equals(this.value);
        }
    }

    public static class ValueAndTimeComparator implements Comparator<ValueAndTime> {
        @Override
        public int compare(ValueAndTime o1, ValueAndTime o2) {
            if (o1.timestamp != o2.timestamp) {
                return (o1.timestamp > o2.timestamp) ? 1 : -1;
            }
            if (o1.value.equals(o2.value)) {
                return 0;
            } else {
                // this is not strictly correct, but I don't want to write too many comparators here :-)
                if (o1.hashCode() > o2.hashCode()) {
                    return 1;
                } else {
                    return -1;
                }
            }
        }
    }

    /**
     * map from value to max timestamp for this value.
     */
    private Map<Object, Long> valueMaxTimeMap = new HashMap<>();
    /**
     * map sorted by time(max timestamp for the value) and then value.
     */
    private SortedMap<ValueAndTime, ValueAndTime> timeSortedMap = new TreeMap<>(new ValueAndTimeComparator());
    private long maxTimestamp = 0L;
    private long window;
    private boolean windowSlided;

    /**
     * @param window - milliseconds.
     */
    public DistinctValuesInTimeWindow(long window) {
        this.window = window;
    }

    public void send(Object value, long timestamp) {
        ValueAndTime vt = new ValueAndTime(value, timestamp);

        // todo think of time out of order
        if (valueMaxTimeMap.containsKey(value)) {
            // remove that entry with old timestamp in timeSortedMap
            long oldTime = valueMaxTimeMap.get(value);
            if (oldTime >= timestamp) {
                // no any effect as the new timestamp is equal or even less than old timestamp
                return;
            }
            timeSortedMap.remove(new ValueAndTime(value, oldTime));
        }
        // insert entry with new timestamp in timeSortedMap
        timeSortedMap.put(vt, vt);
        // update new timestamp in valueMaxTimeMap
        valueMaxTimeMap.put(value, timestamp);

        // evict old entries
        // store max timestamp if possible
        maxTimestamp = Math.max(maxTimestamp, timestamp);

        // check if some values should be evicted because of time window
        Iterator<Map.Entry<ValueAndTime, ValueAndTime>> it = timeSortedMap.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<ValueAndTime, ValueAndTime> entry = it.next();
            if (entry.getKey().timestamp < maxTimestamp - window) {
                // should remove the entry in valueMaxTimeMap and timeSortedMap
                valueMaxTimeMap.remove(entry.getKey().value);
                windowSlided = true;

                it.remove();
            } else {
                break;
            }
        }
    }

    public Map<Object, Long> distinctValues() {
        return valueMaxTimeMap;
    }

    public boolean windowSlided() {
        return windowSlided;
    }
}
