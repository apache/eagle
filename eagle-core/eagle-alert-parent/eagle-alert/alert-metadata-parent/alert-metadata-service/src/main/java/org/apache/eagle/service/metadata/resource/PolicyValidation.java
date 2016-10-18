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
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.eagle.alert.engine.coordinator.PolicyDefinition;

@JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)
public class PolicyValidation {
    private boolean success;
    private String message;
    private String exception;

    private PolicyExecutionPlan policyExecutionPlan;
    private PolicyDefinition policyDefinition;

    public String getException() {
        return exception;
    }

    public void setException(String exception) {
        this.exception = exception;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public void setStackTrace(Throwable throwable) {
        this.setException(ExceptionUtils.getStackTrace(throwable));
    }

    public boolean isSuccess() {
        return success;
    }

    public void setSuccess(boolean success) {
        this.success = success;
    }

    public PolicyExecutionPlan getPolicyExecutionPlan() {
        return policyExecutionPlan;
    }

    public void setPolicyExecutionPlan(PolicyExecutionPlan policyExecutionPlan) {
        this.policyExecutionPlan = policyExecutionPlan;
    }

    public PolicyDefinition getPolicyDefinition() {
        return policyDefinition;
    }

    public void setPolicyDefinition(PolicyDefinition policyDefinition) {
        this.policyDefinition = policyDefinition;
    }
}