/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.eagle.jpm.util;

/**
 * Created by jnwang on 2016/4/27.
 */
public class Constants {

    public final static String SPARK_APP_SERVICE_ENDPOINT_NAME = "SparkAppService";
    public final static String SPARK_JOB_SERVICE_ENDPOINT_NAME = "SparkJobService";
    public final static String SPARK_STAGE_SERVICE_ENDPOINT_NAME = "SparkStageService";
    public final static String SPARK_TASK_SERVICE_ENDPOINT_NAME = "SparkTaskService";
    public final static String SPARK_EXECUTOR_SERVICE_ENDPOINT_NAME = "SparkExecutorService";

    public static final String APPLICATION_PREFIX = "application";
    public static final String JOB_PREFIX = "job";
    public static final String V2_APPS_URL = "ws/v1/cluster/apps";
    public static final String ANONYMOUS_PARAMETER = "anonymous=true";

    public static final String V2_APPS_RUNNING_URL = "ws/v1/cluster/apps?state=RUNNING";
    public static final String V2_APPS_COMPLETED_URL = "ws/v1/cluster/apps?state=FINISHED";

    public static final String SPARK_APPS_URL ="api/v1/applications";

    public enum CompressionType {
        GZIP, NONE
    }
    public enum JobState {
        RUNNING, COMPLETED, ALL
    }

    public enum ResourceType {
         COMPLETE_SPARK_JOB, SPARK_JOB_DETAIL
    }

}
