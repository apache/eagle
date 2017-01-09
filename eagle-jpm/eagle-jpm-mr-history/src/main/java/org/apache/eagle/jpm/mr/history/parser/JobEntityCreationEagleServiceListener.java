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

import org.apache.eagle.dataproc.impl.storm.ValuesArray;
import org.apache.eagle.jpm.mr.history.MRHistoryJobConfig;
import org.apache.eagle.jpm.mr.history.crawler.EagleOutputCollector;
import org.apache.eagle.jpm.mr.history.metrics.JobExecutionMetricsCreationListener;
import org.apache.eagle.jpm.mr.history.publisher.StreamPublisher;
import org.apache.eagle.jpm.mr.history.publisher.StreamPublisherManager;
import org.apache.eagle.jpm.mr.history.zkres.JobHistoryZKStateManager;
import org.apache.eagle.jpm.mr.historyentity.*;
import org.apache.eagle.jpm.mr.historyentity.JobExecutionAPIEntity;
import org.apache.eagle.jpm.mr.historyentity.TaskAttemptExecutionAPIEntity;
import org.apache.eagle.jpm.mr.historyentity.TaskExecutionAPIEntity;
import org.apache.eagle.jpm.util.Constants;
import org.apache.eagle.jpm.util.MRJobTagName;
import org.apache.eagle.log.entity.GenericMetricEntity;
import org.apache.eagle.log.entity.GenericServiceAPIResponseEntity;
import org.apache.eagle.service.client.IEagleServiceClient;
import org.apache.eagle.service.client.impl.EagleServiceClientImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class JobEntityCreationEagleServiceListener implements HistoryJobEntityCreationListener {
    private static final Logger logger = LoggerFactory.getLogger(JobEntityCreationEagleServiceListener.class);
    private static final int BATCH_SIZE = 1000;
    private int batchSize;
    private List<JobBaseAPIEntity> list = new ArrayList<>();
    List<JobExecutionAPIEntity> jobs = new ArrayList<>();
    List<JobEventAPIEntity> jobEvents = new ArrayList<>();
    List<TaskExecutionAPIEntity> taskExecs = new ArrayList<>();
    List<TaskAttemptExecutionAPIEntity> taskAttemptExecs = new ArrayList<>();
    List<TaskAttemptErrorCategoryEntity> taskAttemptErrors = new ArrayList<>();
    private JobExecutionMetricsCreationListener jobExecutionMetricsCreationListener = new JobExecutionMetricsCreationListener();
    private TimeZone timeZone;
    private MRHistoryJobConfig appConfig;

    public JobEntityCreationEagleServiceListener(MRHistoryJobConfig appConfig) {
        this(BATCH_SIZE, appConfig);
    }

    public JobEntityCreationEagleServiceListener(int batchSize, MRHistoryJobConfig appConfig) {
        if (batchSize <= 0) {
            throw new IllegalArgumentException("batchSize must be greater than 0 when it is provided");
        }
        this.batchSize = batchSize;
        this.appConfig = appConfig;
        timeZone = TimeZone.getTimeZone(appConfig.getJobHistoryEndpointConfig().timeZone);
    }

    @Override
    public void jobEntityCreated(JobBaseAPIEntity entity) throws Exception {
        list.add(entity);
        if (list.size() % batchSize == 0) {
            flush();
            list.clear();
        }
    }

    private String timeStamp2Date(long timeStamp) {
        GregorianCalendar cal = new GregorianCalendar(timeZone);
        cal.setTimeInMillis(timeStamp);
        return  String.format("%4d%02d%02d",
            cal.get(GregorianCalendar.YEAR),
            cal.get(GregorianCalendar.MONTH) + 1,
            cal.get(GregorianCalendar.DAY_OF_MONTH));
    }

    /**
     * We need save network bandwidth as well.
     */
    @Override
    public void flush() throws Exception {
        MRHistoryJobConfig.EagleServiceConfig eagleServiceConfig = appConfig.getEagleServiceConfig();
        IEagleServiceClient client = new EagleServiceClientImpl(
            eagleServiceConfig.eagleServiceHost,
            eagleServiceConfig.eagleServicePort,
            eagleServiceConfig.username,
            eagleServiceConfig.password);

        client.setReadTimeout(eagleServiceConfig.readTimeoutSeconds * 1000);
        logger.info("start flushing entities of total number " + list.size());
        List<GenericMetricEntity> metricEntities = new ArrayList<>();
        for (int i = 0; i < list.size(); i++) {
            JobBaseAPIEntity entity = list.get(i);
            if (entity instanceof JobExecutionAPIEntity) {
                jobs.add((JobExecutionAPIEntity) entity);
                JobHistoryZKStateManager.instance().updateProcessedJob(timeStamp2Date(entity.getTimestamp()),
                    entity.getTags().get(MRJobTagName.JOB_ID.toString()),
                    ((JobExecutionAPIEntity) entity).getCurrentState());

                metricEntities.addAll(jobExecutionMetricsCreationListener.generateMetrics((JobExecutionAPIEntity)entity));
                StreamPublisher streamPublisher = StreamPublisherManager.getInstance().getStreamPublisher(JobExecutionAPIEntity.class);
                if (streamPublisher != null) {
                    streamPublisher.flush((JobExecutionAPIEntity) entity);
                }
            } else if (entity instanceof JobEventAPIEntity) {
                jobEvents.add((JobEventAPIEntity) entity);
            } else if (entity instanceof TaskExecutionAPIEntity) {
                taskExecs.add((TaskExecutionAPIEntity) entity);
            } else if (entity instanceof TaskAttemptExecutionAPIEntity) {
                taskAttemptExecs.add((TaskAttemptExecutionAPIEntity) entity);
                StreamPublisher streamPublisher = StreamPublisherManager.getInstance().getStreamPublisher(TaskAttemptExecutionAPIEntity.class);
                if (streamPublisher != null) {
                    streamPublisher.flush((TaskAttemptExecutionAPIEntity) entity);
                }
            } else if (entity instanceof TaskAttemptErrorCategoryEntity) {
                taskAttemptErrors.add((TaskAttemptErrorCategoryEntity) entity);
            }
        }
        GenericServiceAPIResponseEntity result;
        if (jobs.size() > 0) {
            logger.info("flush JobExecutionAPIEntity of number " + jobs.size());
            result = client.create(jobs);
            checkResult(result);
            jobs.clear();
        }
        if (metricEntities.size() > 0) {
            logger.info("flush job metrics of number " + metricEntities.size());
            result = client.create(metricEntities);
            checkResult(result);
            metricEntities.clear();
        }
        if (jobEvents.size() > 0) {
            logger.info("flush JobEventAPIEntity of number " + jobEvents.size());
            result = client.create(jobEvents);
            checkResult(result);
            jobEvents.clear();
        }
        if (taskExecs.size() > 0) {
            logger.info("flush TaskExecutionAPIEntity of number " + taskExecs.size());
            result = client.create(taskExecs);
            checkResult(result);
            taskExecs.clear();
        }
        if (taskAttemptExecs.size() > 0) {
            logger.info("flush TaskAttemptExecutionAPIEntity of number " + taskAttemptExecs.size());
            result = client.create(taskAttemptExecs);
            checkResult(result);
            taskAttemptExecs.clear();
        }
        if (taskAttemptErrors.size() > 0) {
            logger.info("flush TaskAttemptErrorCategoryEntity of number " + taskAttemptErrors.size());
            result = client.create(taskAttemptErrors);
            checkResult(result);
            taskAttemptErrors.clear();
        }

        logger.info("finish flushing entities of total number " + list.size());
        list.clear();
        client.close();
    }

    private void checkResult(GenericServiceAPIResponseEntity result) throws Exception {
        if (!result.isSuccess()) {
            logger.error(result.getException());
            throw new Exception("Entity creation fails going to EagleService");
        }
    }
}
