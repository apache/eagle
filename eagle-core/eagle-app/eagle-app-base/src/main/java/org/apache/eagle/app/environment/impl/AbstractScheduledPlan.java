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
import com.typesafe.config.Config;
import org.quartz.*;

import java.util.Date;

public abstract class AbstractScheduledPlan implements ScheduledPlan {
    private static final String APP_CONFIG_KEY = "appConfig";
    private static final String APP_ID_KEY = "appID";

    private final String appId;
    private final ScheduledEnvironment environment;
    private final Config config;

    public AbstractScheduledPlan(Config config, ScheduledEnvironment environment) {
        this.appId = config.getString("appId");
        Preconditions.checkNotNull(appId, "[appId] is null");
        this.environment = environment;
        this.config = config;
    }

    @Override
    public String getAppId() {
        return this.appId;
    }

    protected Date addJob(JobDetail jobDetail, Trigger trigger) throws SchedulerException {
        return this.environment.scheduler().scheduleJob(jobDetail, trigger);
    }

    protected Date addJob(Trigger trigger) throws SchedulerException {
        return this.environment.scheduler().scheduleJob(trigger);
    }

    protected boolean removeJob(JobKey jobKey, TriggerKey triggerKey) throws SchedulerException {
        return environment.scheduler().unscheduleJob(triggerKey) && environment.scheduler().deleteJob(jobKey);
    }

    protected boolean checkJob(JobKey jobKey) throws SchedulerException {
        return environment.scheduler().checkExists(jobKey);
    }

    protected JobDataMap getDefaultJobDataMap() {
        JobDataMap jobDataMap =  new JobDataMap();
        jobDataMap.put(APP_CONFIG_KEY, this.config);
        jobDataMap.put(APP_ID_KEY, this.getAppId());
        return jobDataMap;
    }

    public static class JobConfig {
        private final Config config;
        private final String appId;

        public JobConfig(JobExecutionContext jobExecutionContext) {
            this.config = (Config) jobExecutionContext.getMergedJobDataMap().get(AbstractScheduledPlan.APP_CONFIG_KEY);
            this.appId = jobExecutionContext.getMergedJobDataMap().getString(AbstractScheduledPlan.APP_ID_KEY);
        }

        public Config getConfig() {
            return config;
        }

        public String getAppId() {
            return appId;
        }
    }
}