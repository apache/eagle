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

import org.apache.eagle.jpm.mr.runningentity.TaskExecutionAPIEntity;
import org.apache.hadoop.hbase.master.snapshot.TakeSnapshotHandler;
import org.apache.hadoop.mapred.Task;

import java.util.*;

class MRJobTaskGroupResponse {
    public List<UnitTaskCount> runningTaskCount;
    public List<UnitTaskCount> finishedTaskCount;
    String errMessage;

   static class UnitTaskCount {
        public long timeBucket;
        public int taskCount;
        public Set<TaskExecutionAPIEntity> entities;
        public List<TaskExecutionAPIEntity> topEntities;

        UnitTaskCount(long timeBucket) {
            this.timeBucket = timeBucket;
            this.taskCount = 0;
            entities = new TreeSet<>(new TaskComparator());
            topEntities = new ArrayList<>();
        }
    }

    static class TaskComparator implements Comparator<TaskExecutionAPIEntity> {
        @Override
        public int compare(TaskExecutionAPIEntity o1, TaskExecutionAPIEntity o2) {
            return (o1.getElapsedTime() > o2.getElapsedTime()) ? -1 : (o1.getElapsedTime() == o2.getElapsedTime() ? 0 : 1);
        }
    }
}
