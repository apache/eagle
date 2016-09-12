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


import org.apache.eagle.jpm.mr.historyentity.TaskExecutionAPIEntity;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class MRTaskExecutionResponse {
    public String errMessage;

    public static class TaskGroupResponse extends MRTaskExecutionResponse {
        public Map<String, TaskGroup> tasksGroupByType;
    }

    public static class TaskGroup {
        public List<TaskExecutionAPIEntity> longTasks;
        public List<TaskExecutionAPIEntity> shortTasks;

        public TaskGroup() {
            longTasks = new ArrayList<>();
            shortTasks = new ArrayList<>();
        }
    }

    public static class JobSuggestionResponse extends MRTaskExecutionResponse {
        public String suggestionType;
        public List<SuggestionResult> suggestionResults;
    }

    public static class SuggestionResult {
        public String name;
        public double value;
        public String suggestion;

        public SuggestionResult(String name, double value, String suggestion) {
            this.name = name;
            this.value = value;
            this.suggestion = suggestion;
        }

        public SuggestionResult(String name, double value) {
            this.value = value;
            this.name = name;
        }
    }

    public static class TaskDistributionResponse extends MRTaskExecutionResponse {
        public String counterName;
        public List<CountUnit> taskBuckets;

        public TaskDistributionResponse() {
            taskBuckets = new ArrayList<>();
        }
    }

    public static class CountUnit {
        public long bucket;
        public long countVal;

        public CountUnit(long bucket) {
            this.bucket = bucket;
            this.countVal = 0;
        }
    }
}
