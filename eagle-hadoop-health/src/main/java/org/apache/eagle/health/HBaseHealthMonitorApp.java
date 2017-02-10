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
package org.apache.eagle.health;

import com.typesafe.config.Config;
import org.apache.eagle.app.ScheduledApplication;
import org.apache.eagle.app.job.MonitorJob;
import org.apache.eagle.app.environment.impl.AbstractSchedulingPlan;
import org.apache.eagle.app.environment.impl.ScheduledEnvironment;
import org.apache.eagle.app.environment.impl.SchedulingPlan;
import org.apache.eagle.health.jobs.HBaseHealthCheckJob;
import org.quartz.*;

public class HBaseHealthMonitorApp extends ScheduledApplication {
    private static final String HBASE_SERVICE_CHECK_JOB_NAME = "HBASE_SERVICE_CHECK_JOB";
    private static final String HBASE_HEALTH_CHECK_JOB_TRIGGER_NAME = "HBASE_SERVICE_CHECK_JOB_TRIGGER";

    @Override
    public SchedulingPlan execute(Config config, ScheduledEnvironment environment) {
        return new AbstractSchedulingPlan(config, environment) {
            @Override
            public void schedule() throws SchedulerException {
                // Schedule Job: HBASE_SERVICE_CHECK_JOB
                scheduleJob(JobBuilder.newJob(HBaseHealthCheckJob.class)
                        .withIdentity(JobKey.jobKey(HBASE_SERVICE_CHECK_JOB_NAME + "_" + getAppId(), MonitorJob.HEALTH_CHECK_JOBS_GROUP))
                        .build(),
                    TriggerBuilder.newTrigger()
                        .withIdentity(TriggerKey.triggerKey(HBASE_HEALTH_CHECK_JOB_TRIGGER_NAME + "_" + getAppId(), MonitorJob.HEALTH_CHECK_JOBS_GROUP))
                        .usingJobData(getJobDataMap(config.getConfig("hbaseHealth")))
                        .startNow()
                        .withSchedule(SimpleScheduleBuilder.repeatSecondlyForever(config.getInt("hbaseHealth.intervalSec"))).build()
                );
            }

            @Override
            public boolean unschedule() throws SchedulerException {
                return removeJob(JobKey.jobKey(HBASE_SERVICE_CHECK_JOB_NAME + "_" + getAppId(), MonitorJob.HEALTH_CHECK_JOBS_GROUP),
                    TriggerKey.triggerKey(HBASE_HEALTH_CHECK_JOB_TRIGGER_NAME + "_" + getAppId(), MonitorJob.HEALTH_CHECK_JOBS_GROUP));
            }

            @Override
            public boolean scheduling() throws SchedulerException {
                return checkJob(JobKey.jobKey(HBASE_SERVICE_CHECK_JOB_NAME, getAppId()));
            }
        };
    }
}
