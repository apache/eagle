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
package org.apache.eagle.app.environment;

import com.google.inject.AbstractModule;
import com.typesafe.config.Config;
import org.apache.eagle.app.environment.impl.*;
import org.quartz.Scheduler;

public class ExecutionRuntimeModule extends AbstractModule {
    private final Config config;

    public ExecutionRuntimeModule(Config config) {
        this.config = config;
    }

    @Override
    protected void configure() {
        bind(StormExecutionRuntime.class).toInstance((StormExecutionRuntime) ExecutionRuntimeManager.getInstance().getRuntimeSingleton(StormEnvironment.class, config));
        // bind(SparkExecutionRuntime.class).toInstance((SparkExecutionRuntime) ExecutionRuntimeManager.getInstance().getRuntimeSingleton(SparkEnvironment.class, config));
        bind(StaticExecutionRuntime.class).toInstance((StaticExecutionRuntime) ExecutionRuntimeManager.getInstance().getRuntimeSingleton(StaticEnvironment.class, config));

        ScheduledExecutionRuntime scheduledExecutionRuntime = (ScheduledExecutionRuntime) ExecutionRuntimeManager.getInstance().getRuntimeSingleton(ScheduledEnvironment.class, config);
        bind(ScheduledExecutionRuntime.class).toInstance(scheduledExecutionRuntime);
        bind(Scheduler.class).toInstance(scheduledExecutionRuntime.environment().scheduler());
    }
}
