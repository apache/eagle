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
package org.apache.eagle.healthcheck;

import com.typesafe.config.Config;
import org.apache.eagle.app.ScheduledApplication;
import org.apache.eagle.app.environment.impl.ScheduledEnvironment;
import org.apache.eagle.app.environment.impl.ScheduledPlan;
import org.apache.eagle.healthcheck.jobs.HBaseHealthCheckJob;
import org.quartz.*;

public class HadoopHealthCheckApp extends ScheduledApplication {
    private static final String HBASE_HEALTHCHECK_JOB_NAME = "HBASE_HEALTHCHECK_JOB";
    private static final String HBASE_HEALTHCHECK_JOB_TRIGGER_NAME = "HBASE_HEALTHCHECK_JOB_TRIGGER";

    @Override
    public ScheduledPlan execute(Config config, ScheduledEnvironment environment) {
        return new ScheduledPlan() {

            @Override
            public void schedule(Scheduler scheduler, String appId) throws SchedulerException {
                JobDetail hbaseHealthCheckJob = JobBuilder
                    .newJob(HBaseHealthCheckJob.class)
                    .withIdentity(JobKey.jobKey(HBASE_HEALTHCHECK_JOB_NAME, appId))
                    .build();
                Trigger hbaseHealthCheckJobTrigger = TriggerBuilder
                    .newTrigger()
                    .withIdentity(TriggerKey.triggerKey(HBASE_HEALTHCHECK_JOB_TRIGGER_NAME, appId))
                    .startNow()
                    .withSchedule(SimpleScheduleBuilder.repeatSecondlyForever(10))
                    .build();
                scheduler.scheduleJob(hbaseHealthCheckJob,hbaseHealthCheckJobTrigger);
            }

            @Override
            public boolean unschedule(Scheduler scheduler, String appId) throws SchedulerException {
                return scheduler.unscheduleJob(TriggerKey.triggerKey(HBASE_HEALTHCHECK_JOB_TRIGGER_NAME, appId))
                    && scheduler.deleteJob(JobKey.jobKey(HBASE_HEALTHCHECK_JOB_NAME, appId));
            }

            @Override
            public boolean scheduling(Scheduler scheduler, String appId) throws SchedulerException {
                return scheduler.checkExists(JobKey.jobKey(HBASE_HEALTHCHECK_JOB_NAME, appId));
            }
        };
    }
}
