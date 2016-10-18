/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/

package org.apache.eagle.jpm.mr.history.parser;

import org.apache.eagle.jpm.mr.history.MRHistoryJobConfig;
import org.apache.eagle.jpm.mr.historyentity.JobBaseAPIEntity;
import org.apache.eagle.jpm.mr.historyentity.JobConfigurationAPIEntity;
import org.apache.eagle.service.client.IEagleServiceClient;
import org.apache.eagle.service.client.impl.EagleServiceClientImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class JobConfigurationCreationServiceListener implements HistoryJobEntityLifecycleListener {
    private static final Logger logger = LoggerFactory.getLogger(JobConfigurationCreationServiceListener.class);
    private static final int MAX_RETRY_TIMES = 3;
    private JobConfigurationAPIEntity jobConfigurationEntity;

    public JobConfigurationCreationServiceListener() {
    }

    @Override
    public void jobEntityCreated(JobBaseAPIEntity entity) throws Exception {
        if (entity != null) {
            if (entity instanceof JobConfigurationAPIEntity) {
                this.jobConfigurationEntity = (JobConfigurationAPIEntity) entity;
            }
        }
    }

    @Override
    public void jobFinish() {

    }

    @Override
    public void flush() throws Exception {
        IEagleServiceClient client = new EagleServiceClientImpl(
            MRHistoryJobConfig.get().getEagleServiceConfig().eagleServiceHost,
            MRHistoryJobConfig.get().getEagleServiceConfig().eagleServicePort,
            MRHistoryJobConfig.get().getEagleServiceConfig().username,
            MRHistoryJobConfig.get().getEagleServiceConfig().password);

        client.getJerseyClient().setReadTimeout(MRHistoryJobConfig.get().getEagleServiceConfig().readTimeoutSeconds * 1000);
        List<JobConfigurationAPIEntity> list = new ArrayList<>();
        list.add(jobConfigurationEntity);

        int tried = 0;
        while (tried <= MAX_RETRY_TIMES) {
            try {
                logger.info("start flushing JobConfigurationAPIEntity entities of total number " + list.size());
                client.create(list);
                logger.info("finish flushing entities of total number " + list.size());
                break;
            } catch (Exception ex) {
                if (tried < MAX_RETRY_TIMES) {
                    logger.error("Got exception to flush, retry as " + (tried + 1) + " times", ex);
                } else {
                    logger.error("Got exception to flush, reach max retry times " + MAX_RETRY_TIMES, ex);
                }
            } finally {
                list.clear();
                jobConfigurationEntity = null;
                client.getJerseyClient().destroy();
                client.close();
            }
            tried++;
        }
    }
}
