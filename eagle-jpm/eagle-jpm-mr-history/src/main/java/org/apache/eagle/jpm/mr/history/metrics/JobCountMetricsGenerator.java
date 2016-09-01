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

import org.apache.commons.lang3.tuple.Pair;
import org.apache.eagle.jpm.mr.history.MRHistoryJobConfig;
import org.apache.eagle.jpm.mr.history.parser.EagleJobStatus;
import org.apache.eagle.jpm.mr.history.zkres.JobHistoryZKStateManager;
import org.apache.eagle.jpm.util.Constants;
import org.apache.eagle.log.entity.GenericMetricEntity;
import org.apache.eagle.service.client.IEagleServiceClient;
import org.apache.eagle.service.client.impl.EagleServiceClientImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class JobCountMetricsGenerator {
    private static final Logger LOG = LoggerFactory.getLogger(JobCountMetricsGenerator.class);

    private MRHistoryJobConfig.EagleServiceConfig eagleServiceConfig;
    private MRHistoryJobConfig.JobExtractorConfig jobExtractorConfig;
    private TimeZone timeZone;

    public JobCountMetricsGenerator(MRHistoryJobConfig.EagleServiceConfig eagleServiceConfig,
                                    MRHistoryJobConfig.JobExtractorConfig jobExtractorConfig,
                                    TimeZone timeZone) {
        this.eagleServiceConfig = eagleServiceConfig;
        this.jobExtractorConfig = jobExtractorConfig;
        this.timeZone = timeZone;
    }

    public void flush(String date, int year, int month, int day) throws Exception {
        List<Pair<String, String>> jobs = JobHistoryZKStateManager.instance().getProcessedJobs(date);
        int total = jobs.size();
        int fail = 0;
        for (Pair<String, String> job : jobs) {
            if (!job.getRight().equals(EagleJobStatus.SUCCEEDED.toString())) {
                ++fail;
            }
        }

        IEagleServiceClient client = new EagleServiceClientImpl(
            eagleServiceConfig.eagleServiceHost,
            eagleServiceConfig.eagleServicePort,
            eagleServiceConfig.username,
            eagleServiceConfig.password);


        GregorianCalendar cal = new GregorianCalendar(year, month, day);
        cal.setTimeZone(timeZone);
        GenericMetricEntity metricEntity = new GenericMetricEntity();
        metricEntity.setTimestamp(cal.getTimeInMillis());
        metricEntity.setPrefix(Constants.JOB_COUNT_PER_DAY);
        metricEntity.setValue(new double[]{total, fail});
        @SuppressWarnings("serial")
        Map<String, String> baseTags = new HashMap<String, String>() {
            {
                put("site", jobExtractorConfig.site);
            }
        };
        metricEntity.setTags(baseTags);
        List<GenericMetricEntity> entities = new ArrayList<>();
        entities.add(metricEntity);

        LOG.info("start flushing entities of total number " + entities.size());
        client.create(entities);
        LOG.info("finish flushing entities of total number " + entities.size());
        client.getJerseyClient().destroy();
        client.close();
    }
}
