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
package org.apache.eagle.app.job.listener;

import com.typesafe.config.Config;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.JobListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GlobalMonitorJobListener implements JobListener {
    private static final Logger LOGGER = LoggerFactory.getLogger(GlobalMonitorJobListener.class);

    private final Config config;

    public GlobalMonitorJobListener(Config config) {
        this.config = config;
    }

    @Override
    public String getName() {
        return GlobalMonitorJobListener.class.getSimpleName();
    }

    @Override
    public void jobToBeExecuted(JobExecutionContext jobExecutionContext) {
        LOGGER.debug("Executing job: {}", jobExecutionContext.getJobDetail());
    }

    @Override
    public void jobExecutionVetoed(JobExecutionContext jobExecutionContext) {
        LOGGER.debug("Vetoing job : {}", jobExecutionContext.getJobDetail());
    }

    @Override
    public void jobWasExecuted(JobExecutionContext jobExecutionContext, JobExecutionException e) {
        // if (jobExecutionContext.getResult() instanceof MonitorResult) {
        LOGGER.info("{} received result: {} from job: {}", this.getName(), jobExecutionContext.getResult(), jobExecutionContext.getJobDetail());
        // }
        // TODO: Persist all results
        // TODO: Send failed result as notification
    }

    @Override
    public String toString() {
        return this.getName();
    }
}