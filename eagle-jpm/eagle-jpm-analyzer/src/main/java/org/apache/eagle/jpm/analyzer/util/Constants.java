/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/

package org.apache.eagle.jpm.analyzer.util;

import org.apache.eagle.jpm.analyzer.publisher.Result;

import java.util.HashMap;
import java.util.Map;

public class Constants {
    public static final String HOST_PATH = "service.host";
    public static final String PORT_PATH = "service.port";
    public static final String USERNAME_PATH = "service.username";
    public static final String PASSWORD_PATH = "service.password";
    public static final String CONTEXT_PATH = "service.context";
    public static final String READ_TIMEOUT_PATH = "service.readTimeOutSeconds";

    public static final String META_PATH = "/metadata";
    public static final String ANALYZER_PATH = "/job/analyzer";
    public static final String JOB_DEF_PATH = "jobDefId";
    public static final String JOB_META_PATH = META_PATH + "/{" + JOB_DEF_PATH + "}";

    public static final String PUBLISHER_PATH = "/publisher";
    public static final String USER_PATH = "userId";
    public static final String PUBLISHER_META_PATH = PUBLISHER_PATH + "/{" + USER_PATH + "}";

    public static final String PROCESS_NONE = "PROCESS_NONE";

    public static final String EVALUATOR_TIME_LENGTH_KEY = "evaluator.timeLength";
    public static final int DEFAULT_EVALUATOR_TIME_LENGTH = 7;//7 days

    public static final String ALERT_THRESHOLD_KEY = "alert.threshold";
    public static final Map<Result.ResultLevel, Double> DEFAULT_ALERT_THRESHOLD = new HashMap<Result.ResultLevel, Double>() {
        {
            put(Result.ResultLevel.NOTICE, 0.1);
            put(Result.ResultLevel.WARNING, 0.3);
            put(Result.ResultLevel.CRITICAL, 0.5);
        }
    };

    public static final String DEDUP_INTERVAL_KEY = "alert.dedupInterval"; //seconds
    public static final int DEFAULT_DEDUP_INTERVAL = 300;

    public static final String ANALYZER_REPORT_CONFIG_PATH = "application.analyzerReport";
    public static final String ANALYZER_REPORT_SUBJECT = "Job Performance Alert For Job: %s";

    public static final String ANALYZER_REPORT_DATA_BASIC_KEY = "basic";
    public static final String ANALYZER_REPORT_DATA_EXTEND_KEY = "extend";
}
