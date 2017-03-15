/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.eagle.app.environment.impl;

import org.apache.spark.streaming.StreamingContextState;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import java.util.HashMap;
import java.util.Map;

public class SparkRuntimeResultManager {
    private static final SparkRuntimeResultManager INSTANCE = new SparkRuntimeResultManager();
    private final Map<String, JavaStreamingContext> sparkPipelineResultCache;

    public static SparkRuntimeResultManager getInstance() {
        return INSTANCE;
    }

    private SparkRuntimeResultManager() {
        sparkPipelineResultCache = new HashMap<>();
    }

    synchronized boolean isAppRunning(String appName) {
        return sparkPipelineResultCache.containsKey(appName) && sparkPipelineResultCache.get(appName).getState() == StreamingContextState.ACTIVE;
    }

    synchronized void insertResult(String appName, JavaStreamingContext rs) {

        sparkPipelineResultCache.put(appName, rs);
    }

    synchronized JavaStreamingContext getResult(String appName) {
        return sparkPipelineResultCache.get(appName);
    }

    synchronized void removeResult(String appName) {
        sparkPipelineResultCache.remove(appName);
    }
}
