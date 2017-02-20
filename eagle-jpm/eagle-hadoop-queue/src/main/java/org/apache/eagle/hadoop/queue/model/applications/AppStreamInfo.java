/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.eagle.hadoop.queue.model.applications;

import java.util.HashMap;
import java.util.Map;

public class AppStreamInfo {
    private static final String SITE = "site";
    private static final String ID = "id";
    private static final String USER = "user";
    private static final String NAME = "appName";
    private static final String QUEUE = "queue";
    private static final String STATE = "state";
    private static final String STARTEDTIME = "startTime";
    private static final String ELAPSEDTIME = "elapsedTime";
    private static final String QUEUE_USAGE_PERCENTAGE = "queueUsagePercentage";
    private static final String CLUSTER_USAGE_PERCENTAGE = "clusterUsagePercentage";
    private static final String TRACKING_URL = "trackingUrl";

    public static Map<String, Object> convertAppToStream(App app, String site) {
        Map<String, Object> queueStreamInfo = new HashMap<>();
        queueStreamInfo.put(SITE, site);
        queueStreamInfo.put(ID, app.getId());
        queueStreamInfo.put(USER, app.getUser());
        queueStreamInfo.put(NAME, app.getName());
        queueStreamInfo.put(QUEUE, app.getQueue());
        queueStreamInfo.put(STATE, app.getState());
        queueStreamInfo.put(ELAPSEDTIME, app.getElapsedTime());
        queueStreamInfo.put(STARTEDTIME, app.getStartedTime());
        queueStreamInfo.put(QUEUE_USAGE_PERCENTAGE, app.getQueueUsagePercentage());
        queueStreamInfo.put(CLUSTER_USAGE_PERCENTAGE, app.getClusterUsagePercentage());
        queueStreamInfo.put(TRACKING_URL, app.getTrackingUrl());

        return queueStreamInfo;
    }

}
