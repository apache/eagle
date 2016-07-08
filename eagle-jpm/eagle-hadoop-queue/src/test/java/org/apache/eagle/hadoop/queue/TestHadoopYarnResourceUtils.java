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

package org.apache.eagle.hadoop.queue;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.eagle.hadoop.queue.common.HadoopYarnResourceUtils;
import org.apache.eagle.hadoop.queue.common.YarnClusterResourceURLBuilder;
import org.apache.eagle.hadoop.queue.model.applications.AppsWrapper;
import org.apache.eagle.hadoop.queue.model.clusterMetrics.ClusterMetricsWrapper;
import org.apache.eagle.hadoop.queue.model.scheduler.SchedulerWrapper;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

public class TestHadoopYarnResourceUtils {

    @Test @Ignore
    public void test() {
        Config config = ConfigFactory.load();

        String baseUrl = HadoopYarnResourceUtils.getConfigValue(config, "dataSourceConfig.RMEndPoints", "");
        String clusterMetricUrl = YarnClusterResourceURLBuilder.buildClusterMetricsURL(baseUrl);
        String finishedAppsUrl = YarnClusterResourceURLBuilder.buildFinishedAppsURL(baseUrl);
        String schedulerUrl = YarnClusterResourceURLBuilder.buildSchedulerInfoURL(baseUrl);

        try {
            ClusterMetricsWrapper clusterMetrics = (ClusterMetricsWrapper) HadoopYarnResourceUtils.getObjectFromStreamWithGzip(clusterMetricUrl, ClusterMetricsWrapper.class);
            AppsWrapper appsWrapper = (AppsWrapper) HadoopYarnResourceUtils.getObjectFromStreamWithGzip(finishedAppsUrl, AppsWrapper.class);
            SchedulerWrapper schedulerWrapper = (SchedulerWrapper) HadoopYarnResourceUtils.getObjectFromStreamWithGzip(schedulerUrl, SchedulerWrapper.class);
            Assert.assertTrue(appsWrapper != null);
            Assert.assertTrue(clusterMetrics != null);
            Assert.assertTrue(schedulerWrapper != null);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
