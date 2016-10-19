/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.eagle.service.metadata.resource;

import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import org.apache.eagle.alert.engine.coordinator.StreamColumn;
import org.apache.eagle.alert.engine.coordinator.StreamPartition;
import org.wso2.siddhi.query.api.ExecutionPlan;

import java.util.List;
import java.util.Map;

@JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)
public class PolicyExecutionPlan {
    /**
     * Actual input streams.
     */
    private Map<String, List<StreamColumn>> inputStreams;

    /**
     * Actual output streams.
     */
    private Map<String, List<StreamColumn>> outputStreams;

    /**
     * Execution plan source.
     */
    private String executionPlanSource;

    /**
     * Execution plan.
     */
    private ExecutionPlan internalExecutionPlan;

    private String executionPlanDesc;

    private List<StreamPartition> streamPartitions;

    public String getExecutionPlanSource() {
        return executionPlanSource;
    }

    public void setExecutionPlanSource(String executionPlanSource) {
        this.executionPlanSource = executionPlanSource;
    }

    public ExecutionPlan getInternalExecutionPlan() {
        return internalExecutionPlan;
    }

    public void setInternalExecutionPlan(ExecutionPlan internalExecutionPlan) {
        this.internalExecutionPlan = internalExecutionPlan;
    }

    public String getExecutionPlanDesc() {
        return executionPlanDesc;
    }

    public void setExecutionPlanDesc(String executionPlanDesc) {
        this.executionPlanDesc = executionPlanDesc;
    }

    public List<StreamPartition> getStreamPartitions() {
        return streamPartitions;
    }

    public void setStreamPartitions(List<StreamPartition> streamPartitions) {
        this.streamPartitions = streamPartitions;
    }

    public Map<String, List<StreamColumn>> getInputStreams() {
        return inputStreams;
    }

    public void setInputStreams(Map<String, List<StreamColumn>> inputStreams) {
        this.inputStreams = inputStreams;
    }

    public Map<String, List<StreamColumn>> getOutputStreams() {
        return outputStreams;
    }

    public void setOutputStreams(Map<String, List<StreamColumn>> outputStreams) {
        this.outputStreams = outputStreams;
    }
}