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

package org.apache.eagle.hadoop.queue;

import com.typesafe.config.Config;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.eagle.app.service.impl.ApplicationHealthCheckBase;
import org.apache.eagle.hadoop.queue.common.HadoopClusterConstants;
import org.apache.eagle.jpm.util.Constants;
import org.apache.eagle.log.entity.GenericServiceAPIResponseEntity;
import org.apache.eagle.metadata.model.ApplicationEntity;
import org.apache.eagle.service.client.IEagleServiceClient;
import org.apache.eagle.service.client.impl.EagleServiceClientImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

public class HadoopQueueRunningApplicationHealthCheck extends ApplicationHealthCheckBase {
    private static final Logger LOG = LoggerFactory.getLogger(HadoopQueueRunningApplicationHealthCheck.class);

    private HadoopQueueRunningAppConfig hadoopQueueRunningAppConfig;

    public HadoopQueueRunningApplicationHealthCheck(Config config) {
        super(config);
        this.hadoopQueueRunningAppConfig = new HadoopQueueRunningAppConfig(config);
    }

    @Override
    public Result check() {
        HadoopQueueRunningAppConfig.EagleProps eagleServiceConfig = this.hadoopQueueRunningAppConfig.eagleProps;
        IEagleServiceClient client = new EagleServiceClientImpl(
                eagleServiceConfig.eagleService.host,
                eagleServiceConfig.eagleService.port,
                eagleServiceConfig.eagleService.username,
                eagleServiceConfig.eagleService.password);

        client.getJerseyClient().setReadTimeout(60000);

        try {
            ApplicationEntity.Status status = getApplicationStatus();
            if (!status.toString().equals(ApplicationEntity.Status.RUNNING.toString())) {
                String message = String.format("Application is not running, status is %s", status.toString());
                return Result.unhealthy(message);
            }

            String query = String.format("%s[@site=\"%s\"]<@site>{max(timestamp)}",
                    Constants.GENERIC_METRIC_SERVICE,
                    hadoopQueueRunningAppConfig.eagleProps.site);

            GenericServiceAPIResponseEntity response = client
                    .search(query)
                    .metricName(HadoopClusterConstants.MetricName.HADOOP_CLUSTER_ALLOCATED_MEMORY)
                    .startTime(System.currentTimeMillis() - 2 * 60 * 60000L)
                    .endTime(System.currentTimeMillis())
                    .pageSize(Integer.MAX_VALUE)
                    .send();
            List<Map<List<String>, List<Double>>> results = response.getObj();
            long currentProcessTimeStamp = results.get(0).get("value").get(0).longValue();
            long currentTimeStamp = System.currentTimeMillis();
            long maxDelayTime = DEFAULT_MAX_DELAY_TIME;
            if (hadoopQueueRunningAppConfig.getConfig().hasPath(MAX_DELAY_TIME_KEY)) {
                maxDelayTime = hadoopQueueRunningAppConfig.getConfig().getLong(MAX_DELAY_TIME_KEY);
            }

            if (currentTimeStamp - currentProcessTimeStamp > maxDelayTime) {
                String message = String.format("Current process time is %sms, delay %s hours",
                        currentProcessTimeStamp, (currentTimeStamp - currentProcessTimeStamp) * 1.0 / 60000L / 60);
                return Result.unhealthy(message);
            } else {
                return Result.healthy();
            }
        } catch (Exception e) {
            return Result.unhealthy(ExceptionUtils.getStackTrace(e.getCause()));
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
