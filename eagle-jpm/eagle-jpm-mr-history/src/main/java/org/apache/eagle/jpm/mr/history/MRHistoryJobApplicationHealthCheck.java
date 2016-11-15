/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.eagle.jpm.mr.history;

import com.codahale.metrics.health.HealthCheck;
import com.typesafe.config.Config;
import org.apache.eagle.jpm.mr.historyentity.JobProcessTimeStampEntity;
import org.apache.eagle.jpm.util.Constants;
import org.apache.eagle.log.entity.GenericServiceAPIResponseEntity;
import org.apache.eagle.service.client.IEagleServiceClient;
import org.apache.eagle.service.client.impl.EagleServiceClientImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

public class MRHistoryJobApplicationHealthCheck extends HealthCheck {
    private static final Logger LOG = LoggerFactory.getLogger(MRHistoryJobApplicationHealthCheck.class);

    private MRHistoryJobConfig mrHistoryJobConfig;
    private static final long DEFAULT_MAX_DELAY_TIME = 2 * 60 * 60 * 1000L;
    private static final String MAX_DELAY_TIME_KEY = "application.maxDelayTime";

    public MRHistoryJobApplicationHealthCheck(Config config) {
        mrHistoryJobConfig = MRHistoryJobConfig.newInstance(config);
    }

    @Override
    public Result check() {
        try {
            MRHistoryJobConfig.EagleServiceConfig eagleServiceConfig = mrHistoryJobConfig.getEagleServiceConfig();
            IEagleServiceClient client = new EagleServiceClientImpl(
                    eagleServiceConfig.eagleServiceHost,
                    eagleServiceConfig.eagleServicePort,
                    eagleServiceConfig.username,
                    eagleServiceConfig.password);

            String query = String.format("%s[@site=\"%s\"]<@site>{max(currentTimeStamp)}",
                    Constants.JPA_JOB_PROCESS_TIME_STAMP_NAME,
                    mrHistoryJobConfig.getJobHistoryEndpointConfig().site);

            GenericServiceAPIResponseEntity response = client
                    .search(query)
                    .startTime(0L)
                    .endTime(System.currentTimeMillis())
                    .pageSize(10)
                    .send();

            List<Map<List<String>, List<Double>>> results = response.getObj();
            long currentProcessTimeStamp = results.get(0).get("value").get(0).longValue();
            long currentTimeStamp = System.currentTimeMillis();
            long maxDelayTime = DEFAULT_MAX_DELAY_TIME;
            if (mrHistoryJobConfig.getConfig().hasPath(MAX_DELAY_TIME_KEY)) {
                maxDelayTime = mrHistoryJobConfig.getConfig().getLong(MAX_DELAY_TIME_KEY);
            }

            if (currentTimeStamp - currentProcessTimeStamp > maxDelayTime) {
                String message = String.format("current process time %sms, delay %sms",
                        currentProcessTimeStamp, currentTimeStamp - currentProcessTimeStamp);
                return Result.unhealthy(message);
            } else {
                return Result.healthy();
            }
        } catch (Exception e) {
            return Result.unhealthy(e);
        }
    }
}
