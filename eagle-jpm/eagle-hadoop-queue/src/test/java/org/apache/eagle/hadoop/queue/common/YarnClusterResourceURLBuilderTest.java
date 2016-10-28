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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.eagle.hadoop.queue.common;

import org.junit.Assert;
import org.junit.Test;

public class YarnClusterResourceURLBuilderTest {

    private static final String BASE_URL_WITH_TAILING_SLASH = "https://baseurl.with.tailing.slash:8080/";
    private static final String BASE_URL_WITHOUT_TAILING_SLASH = "https://baseurl.with.tailing.slash:8080";

    private static final String EXPECTED_SCHEDULER_INFO_URL = "https://baseurl.with.tailing.slash:8080/ws/v1/cluster/scheduler?anonymous=true";
    private static final String EXPECTED_CLUSTER_METRICS_URL = "https://baseurl.with.tailing.slash:8080/ws/v1/cluster/metrics?anonymous=true";
    private static final String EXPECTED_RUNNING_APP_URL = "https://baseurl.with.tailing.slash:8080/ws/v1/cluster/apps?state=RUNNING&anonymous=true";
    private static final String EXPECTED_FINISHED_APPS_URL = "https://baseurl.with.tailing.slash:8080/ws/v1/cluster/apps?state=FINISHED&anonymous=true";

    @Test
    public void testBuildSchedulerInfoURLWithBaseURLTailingSlash() {
        String resolvedUrl = YarnClusterResourceURLBuilder.buildSchedulerInfoURL(BASE_URL_WITH_TAILING_SLASH);
        Assert.assertEquals("unexpected url composed for fetching scheduler info", EXPECTED_SCHEDULER_INFO_URL, resolvedUrl);
    }

    @Test
    public void testBuildClusterMetricsURLWithBaseURLTailingSlash() {
        String resolvedUrl = YarnClusterResourceURLBuilder.buildClusterMetricsURL(BASE_URL_WITH_TAILING_SLASH);
        Assert.assertEquals("unexpected url composed for fetching cluster metrics", EXPECTED_CLUSTER_METRICS_URL, resolvedUrl);
    }

    @Test
    public void testBuildRunningAppsURLWithBaseURLTailingSlash() {
        String resolvedUrl = YarnClusterResourceURLBuilder.buildRunningAppsURL(BASE_URL_WITH_TAILING_SLASH);
        Assert.assertEquals("unexpected url composed for fetching running apps", EXPECTED_RUNNING_APP_URL, resolvedUrl);
    }

    @Test
    public void testBuildFinishedAppsURLWithBaseURLTailingSlash() {
        String resolvedUrl = YarnClusterResourceURLBuilder.buildFinishedAppsURL(BASE_URL_WITH_TAILING_SLASH);
        Assert.assertEquals("unexpected url composed for fetching finished apps", EXPECTED_FINISHED_APPS_URL, resolvedUrl);
    }

    @Test
    public void testBuildSchedulerInfoURLWithoutBaseURLTailingSlash() {
        String resolvedUrl = YarnClusterResourceURLBuilder.buildSchedulerInfoURL(BASE_URL_WITHOUT_TAILING_SLASH);
        Assert.assertEquals("unexpected url composed for fetching scheduler info", EXPECTED_SCHEDULER_INFO_URL, resolvedUrl);
    }

    @Test
    public void testBuildClusterMetricsURLWithoutBaseURLTailingSlash() {
        String resolvedUrl = YarnClusterResourceURLBuilder.buildClusterMetricsURL(BASE_URL_WITHOUT_TAILING_SLASH);
        Assert.assertEquals("unexpected url composed for fetching cluster metrics", EXPECTED_CLUSTER_METRICS_URL, resolvedUrl);
    }

    @Test
    public void testBuildRunningAppsURLWithoutBaseURLTailingSlash() {
        String resolvedUrl = YarnClusterResourceURLBuilder.buildRunningAppsURL(BASE_URL_WITHOUT_TAILING_SLASH);
        Assert.assertEquals("unexpected url composed for fetching running apps", EXPECTED_RUNNING_APP_URL, resolvedUrl);
    }

    @Test
    public void testBuildFinishedAppsURLWithoutBaseURLTailingSlash() {
        String resolvedUrl = YarnClusterResourceURLBuilder.buildFinishedAppsURL(BASE_URL_WITHOUT_TAILING_SLASH);
        Assert.assertEquals("unexpected url composed for fetching finished apps", EXPECTED_FINISHED_APPS_URL, resolvedUrl);
    }
}
