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

package org.apache.eagle.jpm.spark.history;

import com.typesafe.config.Config;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.eagle.app.service.impl.ApplicationHealthCheckBase;
import org.apache.eagle.jpm.util.Constants;
import org.apache.eagle.log.entity.GenericServiceAPIResponseEntity;
import org.apache.eagle.metadata.model.ApplicationEntity;
import org.apache.eagle.service.client.IEagleServiceClient;
import org.apache.eagle.service.client.impl.EagleServiceClientImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

public class SparkHistoryJobApplicationHealthCheck extends ApplicationHealthCheckBase {
    private static final Logger LOG = LoggerFactory.getLogger(SparkHistoryJobApplicationHealthCheck.class);

    private SparkHistoryJobAppConfig sparkHistoryJobAppConfig;

    public SparkHistoryJobApplicationHealthCheck(Config config) {
        super(config);
        this.sparkHistoryJobAppConfig = SparkHistoryJobAppConfig.newInstance(config);
    }

    @Override
    public Result check() {
        SparkHistoryJobAppConfig.EagleInfo eagleServiceConfig = sparkHistoryJobAppConfig.eagleInfo;
        IEagleServiceClient client = new EagleServiceClientImpl(
                eagleServiceConfig.host,
                eagleServiceConfig.port,
                eagleServiceConfig.username,
                eagleServiceConfig.password);

        client.getJerseyClient().setReadTimeout(eagleServiceConfig.timeout * 1000);

        String message = "";
        try {
            ApplicationEntity.Status status = getApplicationStatus();
            if (!status.toString().equals(ApplicationEntity.Status.RUNNING.toString())) {
                message += String.format("Application is not RUNNING, status is %s. ", status.toString());
            }

            String query = String.format("%s[@site=\"%s\"]<@site>{max(endTime)}",
                    Constants.SPARK_APP_SERVICE_ENDPOINT_NAME,
                    sparkHistoryJobAppConfig.stormConfig.siteId);

            GenericServiceAPIResponseEntity response = client
                    .search(query)
                    .startTime(System.currentTimeMillis() - 12 * 60 * 60000L)
                    .endTime(System.currentTimeMillis())
                    .pageSize(10)
                    .send();

            List<Map<List<String>, List<Double>>> results = response.getObj();
            long currentProcessTimeStamp = results.get(0).get("value").get(0).longValue();
            long currentTimeStamp = System.currentTimeMillis();
            long maxDelayTime = DEFAULT_MAX_DELAY_TIME;
            if (sparkHistoryJobAppConfig.getConfig().hasPath(MAX_DELAY_TIME_KEY)) {
                maxDelayTime = sparkHistoryJobAppConfig.getConfig().getLong(MAX_DELAY_TIME_KEY);
            }

            if (!message.isEmpty() || currentTimeStamp - currentProcessTimeStamp > maxDelayTime * 3) {
                message += String.format("Current process time is %sms, delay %s hours.",
                        currentProcessTimeStamp, (currentTimeStamp - currentProcessTimeStamp) * 1.0 / 60000L / 60);
                return Result.unhealthy(message);
            } else {
                return Result.healthy();
            }
        } catch (Exception e) {
            return Result.unhealthy(printMessages(message, "An exception was caught when fetch application current process time: ", ExceptionUtils.getStackTrace(e)));
        } finally {
            client.getJerseyClient().destroy();
            try {
                client.close();
            } catch (Exception e) {
                LOG.warn("{}", e);
            }
        }
    }
}
