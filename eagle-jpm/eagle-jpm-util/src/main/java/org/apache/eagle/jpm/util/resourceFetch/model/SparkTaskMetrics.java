/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one or more
 *  * contributor license agreements.  See the NOTICE file distributed with
 *  * this work for additional information regarding copyright ownership.
 *  * The ASF licenses this file to You under the Apache License, Version 2.0
 *  * (the "License"); you may not use this file except in compliance with
 *  * the License.  You may obtain a copy of the License at
 *  *
 *  *    http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package org.apache.eagle.jpm.util.resourcefetch.model;

import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.map.annotate.JsonSerialize;

@JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class SparkTaskMetrics {
    private long executorDeserializeTime;
    private long executorRunTime;
    private long resultSize;
    private long jvmGcTime;
    private long resultSerializationTime;
    private long memoryBytesSpilled;
    private long diskBytesSpilled;
    private SparkTaskInputMetrics inputMetrics;
    private SparkTaskShuffleWriteMetrics shuffleWriteMetrics;
    private SparkTaskShuffleReadMetrics shuffleReadMetrics;

    public long getExecutorDeserializeTime() {
        return executorDeserializeTime;
    }

    public void setExecutorDeserializeTime(long executorDeserializeTime) {
        this.executorDeserializeTime = executorDeserializeTime;
    }

    public long getExecutorRunTime() {
        return executorRunTime;
    }

    public void setExecutorRunTime(long executorRunTime) {
        this.executorRunTime = executorRunTime;
    }

    public long getResultSize() {
        return resultSize;
    }

    public void setResultSize(long resultSize) {
        this.resultSize = resultSize;
    }

    public long getJvmGcTime() {
        return jvmGcTime;
    }

    public void setJvmGcTime(long jvmGcTime) {
        this.jvmGcTime = jvmGcTime;
    }

    public long getResultSerializationTime() {
        return resultSerializationTime;
    }

    public void setResultSerializationTime(long resultSerializationTime) {
        this.resultSerializationTime = resultSerializationTime;
    }

    public long getMemoryBytesSpilled() {
        return memoryBytesSpilled;
    }

    public void setMemoryBytesSpilled(long memoryBytesSpilled) {
        this.memoryBytesSpilled = memoryBytesSpilled;
    }

    public long getDiskBytesSpilled() {
        return diskBytesSpilled;
    }

    public void setDiskBytesSpilled(long diskBytesSpilled) {
        this.diskBytesSpilled = diskBytesSpilled;
    }

    public SparkTaskInputMetrics getInputMetrics() {
        return inputMetrics;
    }

    public void setInputMetrics(SparkTaskInputMetrics inputMetrics) {
        this.inputMetrics = inputMetrics;
    }

    public SparkTaskShuffleWriteMetrics getShuffleWriteMetrics() {
        return shuffleWriteMetrics;
    }

    public void setShuffleWriteMetrics(SparkTaskShuffleWriteMetrics shuffleWriteMetrics) {
        this.shuffleWriteMetrics = shuffleWriteMetrics;
    }

    public SparkTaskShuffleReadMetrics getShuffleReadMetrics() {
        return shuffleReadMetrics;
    }

    public void setShuffleReadMetrics(SparkTaskShuffleReadMetrics shuffleReadMetrics) {
        this.shuffleReadMetrics = shuffleReadMetrics;
    }
}
