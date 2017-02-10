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
package org.apache.eagle.app.job;

import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public abstract class MonitorJob implements Job {
    public static final String HEALTH_CHECK_JOBS_GROUP = "HEALTH_CHECK_JOBS";

    private static final Logger LOGGER = LoggerFactory.getLogger(MonitorJob.class);

    @Override
    public void execute(JobExecutionContext jobExecutionContext) throws JobExecutionException {
        try {
            prepare(jobExecutionContext);
            jobExecutionContext.setResult(execute());
        } catch (MonitorException ex) {
            jobExecutionContext.setResult(ex.getResult());
        } catch (Throwable ex) {
            LOGGER.error(ex.getMessage(), ex);
            jobExecutionContext.setResult(MonitorResult.critical(ex.getMessage(), ex));
        } finally {
            try {
                this.close();
            } catch (Throwable t) {
                LOGGER.error(t.getMessage(), t);
            }
        }
    }

    protected abstract MonitorResult execute() throws JobExecutionException, Exception;

    protected abstract void prepare(JobExecutionContext context) throws JobExecutionException;

    protected abstract void close() throws JobExecutionException;
}