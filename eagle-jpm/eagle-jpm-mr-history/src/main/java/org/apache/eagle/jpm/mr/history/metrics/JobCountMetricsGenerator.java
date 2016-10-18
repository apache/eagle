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
import org.apache.eagle.jpm.mr.history.parser.EagleJobStatus;
import org.apache.eagle.jpm.mr.history.zkres.JobHistoryZKStateManager;
import org.apache.eagle.jpm.util.Constants;
import org.apache.eagle.jpm.util.MRJobTagName;
import org.apache.eagle.log.entity.GenericMetricEntity;
import org.apache.eagle.service.client.IEagleServiceClient;
import org.apache.eagle.service.client.impl.EagleServiceClientImpl;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class JobCountMetricsGenerator {
    private static final Logger LOG = LoggerFactory.getLogger(JobCountMetricsGenerator.class);

    private TimeZone timeZone;

    public JobCountMetricsGenerator(TimeZone timeZone) {
        this.timeZone = timeZone;
    }

    public void flush(String date, int year, int month, int day) throws Exception {
        List<Pair<String, String>> jobs = JobHistoryZKStateManager.instance().getProcessedJobs(date);
        final int total = jobs.size();
        int succeeded = 0;
        int killed = 0;

        for (Pair<String, String> job : jobs) {
            if (job.getRight().equals(EagleJobStatus.KILLED.toString())) {
                ++killed;
            }

            if (job.getRight().equals(EagleJobStatus.SUCCEEDED.toString())) {
                ++succeeded;
            }
        }
        int failed = total - killed - succeeded;

        final IEagleServiceClient client = new EagleServiceClientImpl(
            MRHistoryJobConfig.get().getEagleServiceConfig().eagleServiceHost,
            MRHistoryJobConfig.get().getEagleServiceConfig().eagleServicePort,
            MRHistoryJobConfig.get().getEagleServiceConfig().username,
            MRHistoryJobConfig.get().getEagleServiceConfig().password);


        GregorianCalendar cal = new GregorianCalendar(year, month, day);
        cal.setTimeZone(timeZone);

        List<GenericMetricEntity> entities = new ArrayList<>();
        entities.add(generateEntity(cal, EagleJobStatus.FAILED.toString(), failed));
        entities.add(generateEntity(cal, EagleJobStatus.KILLED.toString(), killed));
        entities.add(generateEntity(cal, EagleJobStatus.SUCCEEDED.toString(), succeeded));

        LOG.info("start flushing entities of total number " + entities.size());
        client.create(entities);
        LOG.info("finish flushing entities of total number " + entities.size());
        client.getJerseyClient().destroy();
        client.close();
    }

    private GenericMetricEntity generateEntity(GregorianCalendar calendar, String state, int count) {
        GenericMetricEntity metricEntity = new GenericMetricEntity();
        metricEntity.setTimestamp(calendar.getTimeInMillis());
        metricEntity.setPrefix(String.format(Constants.HADOOP_HISTORY_TOTAL_METRIC_FORMAT, Constants.JOB_LEVEL, Constants.JOB_COUNT_PER_DAY));
        metricEntity.setValue(new double[] {count});
        @SuppressWarnings("serial")
        Map<String, String> baseTags = new HashMap<String, String>() {
            {
                put("site", MRHistoryJobConfig.get().getJobHistoryEndpointConfig().site);
                put(MRJobTagName.JOB_STATUS.toString(), state);
            }
        };
        metricEntity.setTags(baseTags);

        return metricEntity;
    }
}
