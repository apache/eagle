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

import org.apache.eagle.alert.engine.coordinator.PolicyDefinition;
import org.apache.eagle.alert.engine.coordinator.StreamDefinition;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import org.apache.commons.lang3.exception.ExceptionUtils;

import java.util.Map;

@JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)
public class PolicyValidation {
    private boolean success;
    private String message;
    private String exception;

    private Map<String, StreamDefinition> validInputStreams;
    private Map<String, StreamDefinition> validOutputStreams;
    private PolicyDefinition policyDefinition;
    private String validExecutionPlan;

    public boolean isSuccess() {
        return success;
    }

    public void setSuccess(boolean success) {
        this.success = success;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }


    public String getException() {
        return exception;
    }

    public void setException(String exception) {
        this.exception = exception;
    }

    public void setStackTrace(Throwable throwable) {
        this.exception = ExceptionUtils.getStackTrace(throwable);
    }

    public Map<String, StreamDefinition> getValidOutputStreams() {
        return validOutputStreams;
    }

    public void setValidOutputStreams(Map<String, StreamDefinition> validOutputStreams) {
        this.validOutputStreams = validOutputStreams;
    }

    public Map<String, StreamDefinition> getValidInputStreams() {
        return validInputStreams;
    }

    public void setValidInputStreams(Map<String, StreamDefinition> validInputStreams) {
        this.validInputStreams = validInputStreams;
    }

    public PolicyDefinition getPolicyDefinition() {
        return policyDefinition;
    }

    public void setPolicyDefinition(PolicyDefinition policyDefinition) {
        this.policyDefinition = policyDefinition;
    }

    public String getValidExecutionPlan() {
        return validExecutionPlan;
    }

    public void setValidExecutionPlan(String validExecutionPlan) {
        this.validExecutionPlan = validExecutionPlan;
    }
}