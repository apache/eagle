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

import org.apache.eagle.alert.engine.coordinator.AlertSeverity;

public class HealthCheckError extends RuntimeException {
    private String name;
    private String group;
    private AlertSeverity severity;

    public HealthCheckError() {
        super();
    }

    public HealthCheckError(String message) {
        super(message);
    }

    public HealthCheckError(String message, Throwable cause) {
        super(message, cause);
    }

    public HealthCheckError(Throwable cause) {
        super(cause);
    }

    protected HealthCheckError(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }

    public class WarningError extends HealthCheckError {

    }

    public class CriticalError extends HealthCheckError {

    }

    public class FetalError extends HealthCheckError {

    }

    public class UnknownError extends HealthCheckError {

    }
}
