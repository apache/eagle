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

package org.apache.eagle.app.environment.impl;

import com.typesafe.config.Config;
import org.apache.eagle.app.Application;
import org.apache.eagle.app.environment.ExecutionRuntime;
import org.apache.eagle.app.environment.ExecutionRuntimeProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * WebExecutionRuntime.
 */
public class WebExecutionRuntime implements ExecutionRuntime<WebEnvironment,StaticApplicationExecutor> {
    private static final Logger LOGGER = LoggerFactory.getLogger(WebExecutionRuntime.class);

    private WebEnvironment environment;

    @Override
    public void prepare(WebEnvironment environment) {
        this.environment = environment;
    }

    @Override
    public WebEnvironment environment() {
        return this.environment;
    }

    @Override
    public void start(Application<WebEnvironment, StaticApplicationExecutor> executor, Config config) {
        LOGGER.warn("Starting {}, do nothing",executor);
    }

    @Override
    public void stop(Application<WebEnvironment, StaticApplicationExecutor> executor, Config config) {
        LOGGER.warn("Stopping {}, do nothing",executor);
    }

    @Override
    public void status(Application<WebEnvironment, StaticApplicationExecutor> executor, Config config) {
        LOGGER.warn("Checking status {}, do nothing",executor);
    }

    public static class Provider implements ExecutionRuntimeProvider<WebEnvironment,StaticApplicationExecutor> {
        @Override
        public ExecutionRuntime<WebEnvironment, StaticApplicationExecutor> get() {
            return new WebExecutionRuntime();
        }
    }
}