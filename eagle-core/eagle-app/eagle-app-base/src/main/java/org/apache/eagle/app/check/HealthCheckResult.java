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
package org.apache.eagle.app.check;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.eagle.alert.engine.coordinator.AlertSeverity;

import java.util.HashMap;
import java.util.Map;

public class HealthCheckResult {
    private AlertSeverity status = AlertSeverity.OK;
    private String group;
    private boolean success;
    private String message;
    private Map<String,Object> data = new HashMap<>();
    private String exception;

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

    public Map<String, Object> getData() {
        return data;
    }

    public void setData(Map<String, Object> data) {
        this.data = data;
    }

    public String getException() {
        return exception;
    }

    public void setException(Throwable exception) {
        this.exception = ExceptionUtils.getFullStackTrace(exception);
    }

    @Override
    public String toString() {
        return String.format("status=%s, message=%s, data=%s, exception=%s",
            this.status,this.message, this.data, this.exception);
    }

    public static HealthCheckResult critical(String message, Throwable throwable) {
        HealthCheckResult result = new HealthCheckResult();
        result.setSuccess(false);
        result.setMessage(message);
        result.setException(throwable);
        result.setStatus(AlertSeverity.CRITICAL);
        return result;
    }

    public static HealthCheckResult ok(String message) {
        HealthCheckResult result = new HealthCheckResult();
        result.setSuccess(true);
        result.setMessage(message);
        result.setStatus(AlertSeverity.OK);
        return result;
    }

    public AlertSeverity getStatus() {
        return status;
    }

    public void setStatus(AlertSeverity status) {
        this.status = status;
    }

    public String getGroup() {
        return group;
    }

    public void setGroup(String group) {
        this.group = group;
    }
}
