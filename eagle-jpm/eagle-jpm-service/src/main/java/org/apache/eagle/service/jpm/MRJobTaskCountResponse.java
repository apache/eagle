/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.eagle.service.jpm;

import java.util.*;

public class MRJobTaskCountResponse {
    public String errMessage;

    public static class TaskCountPerJobResponse extends MRJobTaskCountResponse {
        public long topNumber;
        public List<UnitTaskCount> runningTaskCount;
        public List<UnitTaskCount> finishedTaskCount;
    }

    public static class JobCountResponse extends MRJobTaskCountResponse {
        public Set<String> jobTypes;
        public List<UnitJobCount> jobCounts;
    }

    static class UnitTaskCount {
        public long timeBucket;
        public int taskCount;
        public int mapTaskCount;
        public int reduceTaskCount;
        public Set entities;
        public List topEntities;

        UnitTaskCount(long timeBucket, Comparator comparator) {
            this.timeBucket = timeBucket;
            this.taskCount = 0;
            this.mapTaskCount = 0;
            this.reduceTaskCount = 0;
            entities = new TreeSet<>(comparator);
            topEntities = new ArrayList<>();
        }
    }

    static class UnitJobCount {
        public long timeBucket;
        public long jobCount;
        public Map<String, Long> jobCountByType;

        UnitJobCount(long timeBucket) {
            this.timeBucket = timeBucket;
            this.jobCount = 0;
            this.jobCountByType = new HashMap<>();
        }
    }
}
