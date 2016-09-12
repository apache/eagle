/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

package org.apache.eagle.jpm.mr.history.metrics;

import org.apache.eagle.jpm.mr.history.MRHistoryJobConfig;
import org.apache.eagle.jpm.mr.historyentity.TaskExecutionAPIEntity;
import org.apache.eagle.jpm.util.Constants;
import org.apache.eagle.jpm.util.MRJobTagName;
import org.apache.eagle.jpm.util.jobcounter.JobCounters;
import org.apache.eagle.log.entity.GenericMetricEntity;
import org.apache.eagle.service.client.IEagleServiceClient;
import org.apache.eagle.service.client.impl.EagleServiceClientImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class JobCounterMetricsGenerator {
    private static final Logger LOG = LoggerFactory.getLogger(JobCounterMetricsGenerator.class);
    private static final int BATCH_SIZE = 1000;

    private List<List<GenericMetricEntity>> metricEntities = new ArrayList<>();
    //metric, time, value
    private Map<String, Map<Long, Long>> metricValueByMinute = new HashMap<>();

    private List<GenericMetricEntity> lastEntitiesBatch;
    private Map<String, String> baseTags;

    public JobCounterMetricsGenerator() {
        this.lastEntitiesBatch = null;
    }

    public void setBaseTags(Map<String, String> tags) {
        this.baseTags = tags;
    }

    public void taskExecutionEntityCreated(TaskExecutionAPIEntity taskExecutionAPIEntity) {
        JobCounters jobCounters = taskExecutionAPIEntity.getJobCounters();
        if (jobCounters == null || jobCounters.getCounters() == null) {
            LOG.warn("found null job counters, task {}", taskExecutionAPIEntity.getTags().get(MRJobTagName.TASK_ID.toString()));
            return;
        }

        long duration = taskExecutionAPIEntity.getDuration();
        long startTime = taskExecutionAPIEntity.getStartTime();
        long endTime = taskExecutionAPIEntity.getEndTime();

        Map<String, Map<String, Long>> counters = jobCounters.getCounters();
        for (String groupName : counters.keySet()) {
            Map<String, Long> metricValues = counters.get(groupName);
            for (String metric : metricValues.keySet()) {
                if (!metricValueByMinute.containsKey(metric)) {
                    metricValueByMinute.put(metric, new HashMap<>());
                }
                Long value = metricValues.get(metric);
                double avg = value * 1.0 / duration;
                for (long i = startTime; i <= endTime;) {
                    long timeStamp = i / 60000L * 60000L;
                    if (!metricValueByMinute.get(metric).containsKey(timeStamp)) {
                        metricValueByMinute.get(metric).put(timeStamp, 0L);
                    }
                    long valueByEachMinute = metricValueByMinute.get(metric).get(timeStamp);
                    if (endTime >= timeStamp + 60000L) {
                        metricValueByMinute.get(metric).put(timeStamp, valueByEachMinute + (long)(avg * (timeStamp + 60000L - i)));
                    } else {
                        metricValueByMinute.get(metric).put(timeStamp, valueByEachMinute + (long)(avg * (endTime - timeStamp)));
                    }

                    i = timeStamp + 60000L;
                }
            }
        }
    }

    private String buildMetricName(String field) {
        return String.format(Constants.HADOOP_HISTORY_MINUTE_METRIC_FORMAT, Constants.JOB_LEVEL, field);
    }

    public void flush() throws Exception {
        for (String metric : metricValueByMinute.keySet()) {
            Map<Long, Long> valueByMinute = metricValueByMinute.get(metric);
            for (Long timeStamp : valueByMinute.keySet()) {
                GenericMetricEntity metricEntity = new GenericMetricEntity();
                metricEntity.setTimestamp(timeStamp);
                metricEntity.setPrefix(buildMetricName(metric.toLowerCase()));
                metricEntity.setValue(new double[] {valueByMinute.get(timeStamp)});
                metricEntity.setTags(this.baseTags);

                if (this.lastEntitiesBatch == null || this.lastEntitiesBatch.size() > BATCH_SIZE) {
                    this.lastEntitiesBatch = new ArrayList<>();
                    metricEntities.add(this.lastEntitiesBatch);
                }

                this.lastEntitiesBatch.add(metricEntity);
            }
        }

        IEagleServiceClient client = new EagleServiceClientImpl(
            MRHistoryJobConfig.get().getEagleServiceConfig().eagleServiceHost,
            MRHistoryJobConfig.get().getEagleServiceConfig().eagleServicePort,
            MRHistoryJobConfig.get().getEagleServiceConfig().username,
            MRHistoryJobConfig.get().getEagleServiceConfig().password);

        for (List<GenericMetricEntity> entities : metricEntities) {
            LOG.info("start flushing entities of total number " + entities.size());
            client.create(entities);
            LOG.info("finish flushing entities of total number " + entities.size());
            entities.clear();
        }
        client.getJerseyClient().destroy();
        client.close();
        metricEntities.clear();
    }
}
