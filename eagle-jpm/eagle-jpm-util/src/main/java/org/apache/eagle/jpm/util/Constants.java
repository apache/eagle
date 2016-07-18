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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Constants {
    private final static Logger LOG = LoggerFactory.getLogger(Constants.class);

    //SPARK
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
    public static final String SPARK_EXECUTORS_URL = "executors";
    public static final String SPARK_JOBS_URL = "jobs";
    public static final String SPARK_STAGES_URL = "stages";

    public enum CompressionType {
        GZIP, NONE
    }
    public enum JobState {
        RUNNING, COMPLETED, ALL
    }
    public enum AppState {
        RUNNING, FINISHED, FAILED
    }
    public enum AppStatus {
        SUCCEEDED, FAILED
    }
    public enum ResourceType {
         COMPLETE_SPARK_JOB, SPARK_JOB_DETAIL, RUNNING_SPARK_JOB, RUNNING_MR_JOB
    }

    //MR
    public static final String JPA_JOB_CONFIG_SERVICE_NAME = "JobConfigService";
    public static final String JPA_JOB_EVENT_SERVICE_NAME = "JobEventService";
    public static final String JPA_JOB_EXECUTION_SERVICE_NAME = "JobExecutionService";

    public static final String JPA_TASK_ATTEMPT_EXECUTION_SERVICE_NAME = "TaskAttemptExecutionService";
    public static final String JPA_TASK_FAILURE_COUNT_SERVICE_NAME = "TaskFailureCountService";
    public static final String JPA_TASK_ATTEMPT_COUNTER_SERVICE_NAME = "TaskAttemptCounterService";
    public static final String JPA_TASK_EXECUTION_SERVICE_NAME = "TaskExecutionService";
    public static final String JPA_JOB_PROCESS_TIME_STAMP_NAME = "JobProcessTimeStampService";

    public static final String JOB_TASK_TYPE_TAG = "taskType";

    public static class JobConfiguration {
        // job type
        public static final String SCOOBI_JOB = "scoobi.mode";
        public static final String HIVE_JOB = "hive.query.string";
        public static final String PIG_JOB = "pig.script";
        public static final String CASCADING_JOB = "cascading.app.name";
    }

    /**
     * MR task types
     */
    public enum TaskType {
        SETUP, MAP, REDUCE, CLEANUP
    }

    public enum JobType {
        CASCADING("CASCADING"),HIVE("HIVE"),PIG("PIG"),SCOOBI("SCOOBI"),
        NOTAVALIABLE("N/A")
        ;
        private String value;
        JobType(String value){
            this.value = value;
        }
        @Override
        public String toString() {
            return this.value;
        }
    }

    public static final String FILE_SYSTEM_COUNTER = "org.apache.hadoop.mapreduce.FileSystemCounter";
    public static final String TASK_COUNTER = "org.apache.hadoop.mapreduce.TaskCounter";

    public static final String MAP_TASK_ATTEMPT_COUNTER = "MapTaskAttemptCounter";
    public static final String REDUCE_TASK_ATTEMPT_COUNTER = "ReduceTaskAttemptCounter";

    public static final String MAP_TASK_ATTEMPT_FILE_SYSTEM_COUNTER = "MapTaskAttemptFileSystemCounter";
    public static final String REDUCE_TASK_ATTEMPT_FILE_SYSTEM_COUNTER = "ReduceTaskAttemptFileSystemCounter";

    public enum TaskAttemptCounter {
        TASK_ATTEMPT_DURATION,
    }



    private static final String DEFAULT_JOB_CONF_NORM_JOBNAME_KEY = "eagle.job.name";
    private static final String EAGLE_NORM_JOBNAME_CONF_KEY = "eagle.job.normalizedfieldname";

    public static String JOB_CONF_NORM_JOBNAME_KEY = null;

    static {
        if (JOB_CONF_NORM_JOBNAME_KEY == null) {
            JOB_CONF_NORM_JOBNAME_KEY = DEFAULT_JOB_CONF_NORM_JOBNAME_KEY;
        }
        LOG.info("Loaded " + EAGLE_NORM_JOBNAME_CONF_KEY + " : " + JOB_CONF_NORM_JOBNAME_KEY);
    }
}
