/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.eagle.jpm.util;

public enum SparkJobTagName {
    SITE("site"),
    SPARK_APP_ID("sprkAppId"),
    SPARK_APP_ATTEMPT_ID("sprkAppAttemptId"),
    SPARK_APP_NAME("sprkAppName"),
    SPARK_APP_NORM_NAME("normSprkAppName"),
    SPARK_JOB_ID("jobId"),
    SPARK_SATGE_ID("stageId"),
    SPARK_STAGE_ATTEMPT_ID("stageAttemptId"),
    SPARK_TASK_INDEX("taskIndex"),
    SPARK_TASK_ATTEMPT_ID("taskAttemptId"),
    SPARK_USER("user"),
    SPARK_QUEUE("queue"),
    SPARK_EXECUTOR_ID("executorId");


    private String tagName;

    private SparkJobTagName(String tagName) {
        this.tagName = tagName;
    }

    public String toString() {
        return this.tagName;
    }
}
