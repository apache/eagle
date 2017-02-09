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
package org.apache.eagle.servicecheck;

import com.typesafe.config.Config;
import org.apache.eagle.app.ScheduledApplication;
import org.apache.eagle.app.check.HealthCheckJob;
import org.apache.eagle.app.environment.impl.AbstractScheduledPlan;
import org.apache.eagle.app.environment.impl.ScheduledEnvironment;
import org.apache.eagle.app.environment.impl.ScheduledPlan;
import org.apache.eagle.servicecheck.jobs.HBaseHealthCheckJob;
import org.quartz.*;

public class HadoopServiceCheckApp extends ScheduledApplication {
    private static final String HBASE_SERVICE_CHECK_JOB_NAME = "HBASE_SERVICE_CHECK_JOB";
    private static final String HBASE_HEALTH_CHECK_JOB_TRIGGER_NAME = "HBASE_SERVICE_CHECK_JOB_TRIGGER";

    @Override
    public ScheduledPlan execute(Config config, ScheduledEnvironment environment) {
        return new AbstractScheduledPlan(config, environment) {
            @Override
            public void schedule() throws SchedulerException {
                // Schedule Job: HBASE_SERVICE_CHECK_JOB
                scheduleJob(JobBuilder.newJob(HBaseHealthCheckJob.class)
                        .withIdentity(JobKey.jobKey(HBASE_SERVICE_CHECK_JOB_NAME + "_" + getAppId(), HealthCheckJob.HEALTH_CHECK_JOBS_GROUP))
                        .build(),
                    TriggerBuilder.newTrigger()
                        .withIdentity(TriggerKey.triggerKey(HBASE_HEALTH_CHECK_JOB_TRIGGER_NAME + "_" + getAppId(), HealthCheckJob.HEALTH_CHECK_JOBS_GROUP))
                        .usingJobData(getJobDataMap(config.getConfig("serviceCheck.hbaseCheckConfig")))
                        .startNow()
                        .withSchedule(SimpleScheduleBuilder.repeatSecondlyForever(config.getInt("serviceCheck.hbaseCheckConfig.intervalSec"))).build()
                );
            }

            @Override
            public boolean unschedule() throws SchedulerException {
                return removeJob(JobKey.jobKey(HBASE_SERVICE_CHECK_JOB_NAME + "_" + getAppId(), HealthCheckJob.HEALTH_CHECK_JOBS_GROUP),
                    TriggerKey.triggerKey(HBASE_HEALTH_CHECK_JOB_TRIGGER_NAME + "_" + getAppId(), HealthCheckJob.HEALTH_CHECK_JOBS_GROUP));
            }

            @Override
            public boolean scheduling() throws SchedulerException {
                return checkJob(JobKey.jobKey(HBASE_SERVICE_CHECK_JOB_NAME, getAppId()));
            }
        };
    }
}
