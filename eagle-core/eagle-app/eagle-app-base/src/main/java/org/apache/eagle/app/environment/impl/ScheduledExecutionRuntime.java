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

import com.google.common.base.Preconditions;
import com.google.inject.Singleton;
import com.typesafe.config.Config;
import org.apache.eagle.app.Application;
import org.apache.eagle.app.environment.ExecutionRuntime;
import org.apache.eagle.app.environment.ExecutionRuntimeProvider;
import org.apache.eagle.metadata.model.ApplicationEntity;
import org.quartz.SchedulerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Singleton
public class ScheduledExecutionRuntime implements ExecutionRuntime<ScheduledEnvironment, ScheduledPlan> {
    private static final Logger LOGGER = LoggerFactory.getLogger(ScheduledExecutionRuntime.class);
    private ScheduledEnvironment environment;

    @Override
    public void prepare(ScheduledEnvironment environment) {
        this.environment = environment;
    }

    @Override
    public ScheduledEnvironment environment() {
        return this.environment;
    }

    @Override
    public void start(Application<ScheduledEnvironment, ScheduledPlan> executor, Config config) {
        String appId = config.getString("appId");
        Preconditions.checkNotNull(appId, "[appId] is required by null for " + executor.getClass().getCanonicalName());
        ScheduledPlan scheduledPlan = executor.execute(config, this.environment);
        try {
            scheduledPlan.schedule(this.environment.getScheduler(), appId);
        } catch (SchedulerException e) {
            LOGGER.error(e.getMessage(),e);
        }
    }

    @Override
    public void stop(Application<ScheduledEnvironment, ScheduledPlan> executor, Config config) {
        String appId = config.getString("appId");
        Preconditions.checkNotNull(appId, "[appId] is required by null for " + executor.getClass().getCanonicalName());
        ScheduledPlan scheduledPlan = executor.execute(config, this.environment);
        try {
            scheduledPlan.unschedule(this.environment.getScheduler(), appId);
        } catch (SchedulerException e) {
            LOGGER.error(e.getMessage(),e);
        }
    }

    @Override
    public ApplicationEntity.Status status(Application<ScheduledEnvironment, ScheduledPlan> executor, Config config) {
        String appId = config.getString("appId");
        Preconditions.checkNotNull(appId, "[appId] is required by null for " + executor.getClass().getCanonicalName());
        ScheduledPlan scheduledPlan = executor.execute(config, this.environment);
        try {
            if (scheduledPlan.scheduling(this.environment.getScheduler(), appId)) {
                return ApplicationEntity.Status.RUNNING;
            } else {
                return ApplicationEntity.Status.STOPPED;
            }
        } catch (SchedulerException e) {
            LOGGER.error(e.getMessage(),e);
        }
        return ApplicationEntity.Status.UNKNOWN;
    }

    @Singleton
    public static class Provider implements ExecutionRuntimeProvider<ScheduledEnvironment,ScheduledPlan> {
        @Override
        public ExecutionRuntime<ScheduledEnvironment, ScheduledPlan> get() {
            return new ScheduledExecutionRuntime();
        }
    }
}
