/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/

package org.apache.eagle.jpm.util.jobcounter;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;


public final class JobCounters implements Serializable {
    
    private Map<String, Map<String, Long>> counters = new TreeMap<>();

    public Map<String, Map<String, Long>> getCounters() {
        return counters;
    }

    public void setCounters(Map<String, Map<String, Long>> counters) {
        this.counters = counters;
    }
    
    public String toString() {
        return counters.toString();
    }

    public void clear() {
        for (Map.Entry<String, Map<String, Long>> entry : counters.entrySet()) {
            entry.getValue().clear();
        }
        counters.clear();
    }

    public Long getCounterValue(CounterName counterName) {
        if (counters.containsKey(counterName.group.name) &&
                counters.get(counterName.group.name).containsKey(counterName.name)) {
            return counters.get(counterName.group.name).get(counterName.name);
        } else {
            return 0L;
        }
    }

    public static enum GroupName {
        FileSystemCounters("org.apache.hadoop.mapreduce.FileSystemCounter", "FileSystemCounters"),
        MapReduceTaskCounter("org.apache.hadoop.mapreduce.TaskCounter", "MapReduceTaskCounter"),
        MapReduceJobCounter("org.apache.hadoop.mapreduce.JobCounter", "MapReduceJobCounter");

        private String name;
        private String displayName;

        GroupName(String name, String displayName) {
            this.name = name;
            this.displayName = displayName;
        }

        public String getName() {
            return name;
        }

        public String getDisplayName() {
            return displayName;
        }
    }

    public static enum CounterName {

        FILE_BYTES_READ(GroupName.FileSystemCounters, "FILE_BYTES_READ", "FILE_BYTES_READ"),
        FILE_BYTES_WRITTEN(GroupName.FileSystemCounters, "FILE_BYTES_WRITTEN", "FILE_BYTES_WRITTEN"),
        HDFS_BYTES_READ(GroupName.FileSystemCounters, "HDFS_BYTES_READ", "HDFS_BYTES_READ"),
        HDFS_BYTES_WRITTEN(GroupName.FileSystemCounters, "HDFS_BYTES_WRITTEN", "HDFS_BYTES_WRITTEN"),

        MAP_INPUT_RECORDS(GroupName.MapReduceTaskCounter, "MAP_INPUT_RECORDS", "Map input records"),
        MAP_OUTPUT_RECORDS(GroupName.MapReduceTaskCounter, "MAP_OUTPUT_RECORDS", "Map output records"),
        MAP_OUTPUT_BYTES(GroupName.MapReduceTaskCounter, "MAP_OUTPUT_BYTES", "Map output bytes"),
        MAP_OUTPUT_MATERIALIZED_BYTES(GroupName.MapReduceTaskCounter, "MAP_OUTPUT_MATERIALIZED_BYTES", "Map output materialized bytes"),
        SPLIT_RAW_BYTES(GroupName.MapReduceTaskCounter, "SPLIT_RAW_BYTES", "SPLIT_RAW_BYTES"),

        REDUCE_INPUT_GROUPS(GroupName.MapReduceTaskCounter, "REDUCE_INPUT_GROUPS", "Reduce input groups"),
        REDUCE_SHUFFLE_BYTES(GroupName.MapReduceTaskCounter, "REDUCE_SHUFFLE_BYTES", "Reduce shuffle bytes"),
        REDUCE_OUTPUT_RECORDS(GroupName.MapReduceTaskCounter, "REDUCE_OUTPUT_RECORDS", "Reduce output records"),
        REDUCE_INPUT_RECORDS(GroupName.MapReduceTaskCounter, "REDUCE_INPUT_RECORDS", "Reduce input records"),

        COMBINE_INPUT_RECORDS(GroupName.MapReduceTaskCounter, "COMBINE_INPUT_RECORDS", "Combine input records"),
        COMBINE_OUTPUT_RECORDS(GroupName.MapReduceTaskCounter, "COMBINE_OUTPUT_RECORDS", "Combine output records"),
        SPILLED_RECORDS(GroupName.MapReduceTaskCounter, "SPILLED_RECORDS", "Spilled Records"),

        CPU_MILLISECONDS(GroupName.MapReduceTaskCounter, "CPU_MILLISECONDS", "CPU time spent (ms)"),
        GC_MILLISECONDS(GroupName.MapReduceTaskCounter, "GC_TIME_MILLIS", "GC time elapsed (ms)"),
        COMMITTED_HEAP_BYTES(GroupName.MapReduceTaskCounter, "COMMITTED_HEAP_BYTES", "Total committed heap usage (bytes)"),
        PHYSICAL_MEMORY_BYTES(GroupName.MapReduceTaskCounter, "PHYSICAL_MEMORY_BYTES", "Physical memory (bytes) snapshot"),
        VIRTUAL_MEMORY_BYTES(GroupName.MapReduceTaskCounter, "VIRTUAL_MEMORY_BYTES", "Virtual memory (bytes) snapshot");

        private GroupName group;
        private String name;
        private String displayName;

        CounterName(GroupName group, String name, String displayName) {
            this.group = group;
            this.name = name;
            this.displayName = displayName;
        }

        public String getName() {
            return name;
        }

        public String getDisplayName() {
            return displayName;
        }

        public String getGroupName() {
            return group.name();
        }
    }
}
