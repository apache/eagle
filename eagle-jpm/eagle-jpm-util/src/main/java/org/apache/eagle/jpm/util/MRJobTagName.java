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

package org.apache.eagle.jpm.util;

public enum MRJobTagName {
    SITE("site"),
    RACK("rack"),
    HOSTNAME("hostname"),
    JOB_NAME("jobName"),
    JOD_DEF_ID("jobDefId"),
    JOB_ID("jobId"),
    TASK_ID("taskId"),
    TASK_ATTEMPT_ID("taskAttemptId"),
    JOB_STATUS("jobStatus"),
    USER("user"),
    TASK_TYPE("taskType"),
    TASK_EXEC_TYPE("taskExecType"),
    ERROR_CATEGORY("errorCategory"),
    JOB_QUEUE("queue"),
    RULE_TYPE("ruleType"),
    JOB_TYPE("jobType");

    private String tagName;

    private MRJobTagName(String tagName) {
        this.tagName = tagName;
    }

    public String toString() {

        return this.tagName;
    }
}
