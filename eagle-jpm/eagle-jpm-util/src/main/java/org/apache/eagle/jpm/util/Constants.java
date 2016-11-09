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
    private static final Logger LOG = LoggerFactory.getLogger(Constants.class);

    public static final String GENERIC_METRIC_SERVICE = "GenericMetricService";

    //SPARK
    public static final String SPARK_APP_SERVICE_ENDPOINT_NAME = "SparkAppService";
    public static final String SPARK_JOB_SERVICE_ENDPOINT_NAME = "SparkJobService";
    public static final String SPARK_STAGE_SERVICE_ENDPOINT_NAME = "SparkStageService";
    public static final String SPARK_TASK_SERVICE_ENDPOINT_NAME = "SparkTaskService";
    public static final String SPARK_EXECUTOR_SERVICE_ENDPOINT_NAME = "SparkExecutorService";
    public static final String RUNNING_SPARK_APP_SERVICE_ENDPOINT_NAME = "RunningSparkAppService";
    public static final String RUNNING_SPARK_JOB_SERVICE_ENDPOINT_NAME = "RunningSparkJobService";
    public static final String RUNNING_SPARK_STAGE_SERVICE_ENDPOINT_NAME = "RunningSparkStageService";
    public static final String RUNNING_SPARK_TASK_SERVICE_ENDPOINT_NAME = "RunningSparkTaskService";
    public static final String RUNNING_SPARK_EXECUTOR_SERVICE_ENDPOINT_NAME = "RunningSparkExecutorService";
    public static final String APPLICATION_PREFIX = "application";
    public static final String JOB_PREFIX = "job";
    public static final String V2_APPS_URL = "ws/v1/cluster/apps";
    public static final String ANONYMOUS_PARAMETER = "anonymous=true";

    public static final String V2_APPS_RUNNING_URL = "ws/v1/cluster/apps?state=RUNNING";
    public static final String V2_APPS_COMPLETED_URL = "ws/v1/cluster/apps?state=FINISHED";

    public static final String SPARK_MASTER_KEY = "spark.master";
    public static final String SPARK_EXECUTOR_MEMORY_KEY = "spark.executor.memory";
    public static final String SPARK_DRIVER_MEMORY_KEY = "spark.driver.memory";
    public static final String SPARK_YARN_AM_MEMORY_KEY = "spark.yarn.am.memory";
    public static final String SPARK_EXECUTOR_CORES_KEY = "spark.executor.cores";
    public static final String SPARK_DRIVER_CORES_KEY = "spark.driver.cores";
    public static final String SPARK_YARN_AM_CORES_KEY = "spark.yarn.am.cores";
    public static final String SPARK_YARN_EXECUTOR_MEMORY_OVERHEAD_KEY = "spark.yarn.executor.memoryOverhead";
    public static final String SPARK_YARN_DRIVER_MEMORY_OVERHEAD_KEY = "spark.yarn.driver.memoryOverhead";
    public static final String SPARK_YARN_am_MEMORY_OVERHEAD_KEY = "spark.yarn.am.memoryOverhead";

    public static final String SPARK_APPS_URL = "api/v1/applications";
    public static final String SPARK_EXECUTORS_URL = "executors";
    public static final String SPARK_JOBS_URL = "jobs";
    public static final String SPARK_STAGES_URL = "stages";
    public static final String MR_JOBS_URL = "ws/v1/mapreduce/jobs";
    public static final String MR_JOB_COUNTERS_URL = "counters";
    public static final String MR_TASKS_URL = "tasks";
    public static final String MR_TASK_ATTEMPTS_URL = "attempts";
    public static final String MR_CONF_URL = "conf";

    public static final String YARN_API_CLUSTER_INFO = "ws/v1/cluster/info";

    public enum CompressionType {
        GZIP, NONE
    }

    public enum JobState {
        NEW, INITED, RUNNING, SUCCEEDED, FAILED, KILL_WAIT, KILLED, ERROR, FINISHED, ALL
    }

    public enum TaskState {
        NEW, SCHEDULED, RUNNING, SUCCEEDED, FAILED, KILL_WAIT, KILLED
    }

    public enum StageState {
        ACTIVE, COMPLETE, PENDING
    }

    public enum AppState {
        NEW, NEW_SAVING, SUBMITTED, ACCEPTED, RUNNING, FINISHED, FAILED, KILLED
    }

    public enum AppStatus {
        UNDEFINED, SUCCEEDED, FAILED, KILLED
    }

    public enum ResourceType {
        COMPLETE_SPARK_JOB, SPARK_JOB_DETAIL, RUNNING_SPARK_JOB, RUNNING_MR_JOB, CLUSTER_INFO, JOB_CONFIGURATION,
        COMPLETE_MR_JOB
    }

    public static enum SuggestionType {
        MapInput, ReduceInput, MapSpill, MapGC, ReduceGC;
    }

    public static final String TASK_RUNNING = "RUNNING";
    public static final String TASK_FINISHED = "FINISHED";

    //MR
    public static final String JPA_JOB_CONFIG_SERVICE_NAME = "JobConfigService";
    public static final String JPA_JOB_EVENT_SERVICE_NAME = "JobEventService";
    public static final String JPA_JOB_EXECUTION_SERVICE_NAME = "JobExecutionService";
    public static final String JPA_JOB_COUNT_SERVICE_NAME = "JobCountService";
    public static final String JPA_RUNNING_JOB_EXECUTION_SERVICE_NAME = "RunningJobExecutionService";
    public static final String JPA_TASK_ATTEMPT_EXECUTION_SERVICE_NAME = "TaskAttemptExecutionService";
    public static final String JPA_TASK_FAILURE_COUNT_SERVICE_NAME = "TaskFailureCountService";
    public static final String JPA_TASK_ATTEMPT_COUNTER_SERVICE_NAME = "TaskAttemptCounterService";
    public static final String JPA_TASK_EXECUTION_SERVICE_NAME = "TaskExecutionService";
    public static final String JPA_RUNNING_TASK_EXECUTION_SERVICE_NAME = "RunningTaskExecutionService";
    public static final String JPA_RUNNING_TASK_ATTEMPT_EXECUTION_SERVICE_NAME = "RunningTaskAttemptExecutionService";
    public static final String JPA_JOB_PROCESS_TIME_STAMP_NAME = "JobProcessTimeStampService";

    public static final String JOB_TASK_TYPE_TAG = "taskType";

    public static class JobConfiguration {
        // job type
        public static final String SCOOBI_JOB = "scoobi.mode";
        public static final String HIVE_JOB = "hive.query.string";
        public static final String PIG_JOB = "pig.script";
        public static final String CASCADING_JOB = "cascading.app.name";
    }

    public static final String HIVE_QUERY_STRING = "hive.query.string";

    /**
     * MR task types.
     */
    public enum TaskType {
        SETUP, MAP, REDUCE, CLEANUP
    }

    public enum JobType {
        CASCADING("CASCADING"),HIVE("HIVE"),PIG("PIG"),SCOOBI("SCOOBI"),
        NOTAVALIABLE("N/A")
        ;
        private String value;
        JobType(String value) {
            this.value = value;
        }

        @Override
        public String toString() {
            return this.value;
        }
    }

    public static final String FILE_SYSTEM_COUNTER = "org.apache.hadoop.mapreduce.FileSystemCounter";
    public static final String TASK_COUNTER = "org.apache.hadoop.mapreduce.TaskCounter";
    public static final String JOB_COUNTER = "org.apache.hadoop.mapreduce.JobCounter";

    public static final String MAP_TASK_ATTEMPT_COUNTER = "MapTaskAttemptCounter";
    public static final String REDUCE_TASK_ATTEMPT_COUNTER = "ReduceTaskAttemptCounter";

    public static final String MAP_TASK_ATTEMPT_FILE_SYSTEM_COUNTER = "MapTaskAttemptFileSystemCounter";
    public static final String REDUCE_TASK_ATTEMPT_FILE_SYSTEM_COUNTER = "ReduceTaskAttemptFileSystemCounter";

    public enum TaskAttemptCounter {
        TASK_ATTEMPT_DURATION,
    }

    public enum JobCounter {
        DATA_LOCAL_MAPS,
        RACK_LOCAL_MAPS,
        TOTAL_LAUNCHED_MAPS
    }

    public static final String hadoopMetricFormat = "hadoop.%s.%s";
    public static final String ALLOCATED_MB = "allocatedmb";
    public static final String ALLOCATED_VCORES = "allocatedvcores";
    public static final String RUNNING_CONTAINERS = "runningcontainers";
    public static final String TASK_EXECUTION_TIME = "taskduration";
    public static final String JOB_EXECUTION_TIME = "jobduration";
    public static final String MAP_COUNT_RATIO = "map.count.ratio";
    public static final String REDUCE_COUNT_RATIO = "reduce.count.ratio";
    public static final String JOB_LEVEL = "job";
    public static final String TASK_LEVEL = "task";
    public static final String USER_LEVEL = "user";
    public static final String JOB_COUNT_PER_DAY = "day.count";
    public static final String JOB_COUNT_PER_HOUR = "hour.count";

    public static final String HADOOP_HISTORY_TOTAL_METRIC_FORMAT = "hadoop.%s.history.%s";
    public static final String HADOOP_HISTORY_MINUTE_METRIC_FORMAT = "hadoop.%s.history.minute.%s";

    public static final String JOB_NAME_NORMALIZATION_RULES_KEY_DEFAULT = "^(.*)[0-9]{4}/[0-9]{2}/[0-9]{2}/[0-9]{2}$ => "
        + "$1~ ; ^(oozie:launcher):T=(.*):W=(.*):A=(.*):ID=(?:.*)$ => $1-$2-$3-$4~ ; ^(.*)([0-9]{10})$ => $1~ ; "
        + "^[0-9a-z]{8}-[0-9a-z]{4}-[0-9a-z]{4}-[0-9a-z]{4}-[0-9a-z]{12}$ => uuid~ ; ^(.*)(?:[0-9]{8}/[0-9]{2}_[0-9]{2})$ => $1~";
}
