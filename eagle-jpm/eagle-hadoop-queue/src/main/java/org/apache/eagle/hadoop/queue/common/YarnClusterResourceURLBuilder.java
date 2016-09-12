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

package org.apache.eagle.hadoop.queue.common;

public class YarnClusterResourceURLBuilder {

    private static final String CLUSTER_SCHEDULER_API_URL = "ws/v1/cluster/scheduler";
    private static final String CLUSTER_METRICS_API_URL = "ws/v1/cluster/metrics";
    private static final String CLUSTER_APPS_API_URL = "ws/v1/cluster/apps";
    private static final String ANONYMOUS_PARAMETER = "anonymous=true";

    public static String buildSchedulerInfoURL(String urlBase) {
        return urlBase + CLUSTER_SCHEDULER_API_URL + "?" + ANONYMOUS_PARAMETER;
    }

    public static String buildClusterMetricsURL(String urlBase) {
        return urlBase + CLUSTER_METRICS_API_URL + "?" + ANONYMOUS_PARAMETER;
    }

    public static String buildRunningAppsURL(String urlBase) {
        return urlBase + CLUSTER_APPS_API_URL + "?state=RUNNING" + "&" + ANONYMOUS_PARAMETER;
    }

    public static String buildFinishedAppsURL(String urlBase) {
        return urlBase + CLUSTER_APPS_API_URL + "?state=FINISHED" + "&" + ANONYMOUS_PARAMETER;
    }
}
