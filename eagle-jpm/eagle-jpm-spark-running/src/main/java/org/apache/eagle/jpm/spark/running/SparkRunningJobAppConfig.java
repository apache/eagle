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

package org.apache.eagle.jpm.spark.running;

import com.typesafe.config.ConfigValue;
import com.typesafe.config.Config;
import org.apache.eagle.jpm.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class SparkRunningJobAppConfig implements Serializable {
    private static final Logger LOG = LoggerFactory.getLogger(SparkRunningJobAppConfig.class);
    static final String JOB_FETCH_SPOUT_NAME = "sparkRunningJobFetchSpout";
    static final String JOB_PARSE_BOLT_NAME = "sparkRunningJobParseBolt";

    static final String DEFAULT_SPARK_JOB_RUNNING_ZOOKEEPER_ROOT = "/apps/spark/running";
    static final String JOB_SYMBOL = "/jobs";

    ZKStateConfig getZkStateConfig() {
        return zkStateConfig;
    }

    private ZKStateConfig zkStateConfig;

    public EagleServiceConfig getEagleServiceConfig() {
        return eagleServiceConfig;
    }

    private EagleServiceConfig eagleServiceConfig;

    public JobExtractorConfig getJobExtractorConfig() {
        return jobExtractorConfig;
    }

    private JobExtractorConfig jobExtractorConfig;

    public EndpointConfig getEndpointConfig() {
        return endpointConfig;
    }

    private EndpointConfig endpointConfig;

    public static class ZKStateConfig implements Serializable {
        public String zkQuorum;
        public String zkRoot;
        public int zkSessionTimeoutMs;
        public int zkRetryTimes;
        public int zkRetryInterval;
        public boolean recoverEnabled;
        public String zkLockPath;
    }

    public static class EagleServiceConfig implements Serializable {
        public String eagleServiceHost;
        public int eagleServicePort;
        public int readTimeoutSeconds;
        public int maxFlushNum;
        public String username;
        public String password;
    }

    public static class JobExtractorConfig implements Serializable {
        public String site;
        public int fetchRunningJobInterval;
        public int parseThreadPoolSize;
        public int jobFetchSpoutParallism;
        public int jobFetchSpoutTasksNum;
        public int jobParseBoltParallism;
        public int jobParseBoltTasksNum;
    }

    public static class EndpointConfig implements Serializable {
        public String eventLog;
        public String[] rmUrls;
        public Map<String, String> hdfs;
    }

    public Config getConfig() {
        return config;
    }

    private Config config;

    private SparkRunningJobAppConfig(Config config) {
        this.eagleServiceConfig = new EagleServiceConfig();
        this.jobExtractorConfig = new JobExtractorConfig();
        this.endpointConfig = new EndpointConfig();
        this.endpointConfig.hdfs = new HashMap<>();
        this.zkStateConfig = new ZKStateConfig();
        init(config);
    }

    public static SparkRunningJobAppConfig newInstance(Config config) {
        return new SparkRunningJobAppConfig(config);
    }

    private void init(Config config) {
        this.config = config;

        this.zkStateConfig.zkQuorum = config.getString("zookeeper.zkQuorum");
        this.zkStateConfig.zkRetryInterval = config.getInt("zookeeper.zkRetryInterval");
        this.zkStateConfig.zkRetryTimes = config.getInt("zookeeper.zkRetryTimes");
        this.zkStateConfig.zkSessionTimeoutMs = config.getInt("zookeeper.zkSessionTimeoutMs");
        this.zkStateConfig.zkLockPath = Utils.makeLockPath(DEFAULT_SPARK_JOB_RUNNING_ZOOKEEPER_ROOT + "/" + config.getString("siteId"));
        this.zkStateConfig.zkRoot = DEFAULT_SPARK_JOB_RUNNING_ZOOKEEPER_ROOT + "/" + config.getString("siteId") + JOB_SYMBOL;
        if (config.hasPath("zookeeper.zkRoot")) {
            this.zkStateConfig.zkRoot = config.getString("zookeeper.zkRoot");
        }
        this.zkStateConfig.recoverEnabled = false;
        if (config.hasPath("jobExtractorConfig.recoverEnabled")) {
            this.zkStateConfig.recoverEnabled = config.getBoolean("jobExtractorConfig.recoverEnabled");
        }

        // parse eagle service endpoint
        this.eagleServiceConfig.eagleServiceHost = config.getString("service.host");
        this.eagleServiceConfig.eagleServicePort = config.getInt("service.port");
        this.eagleServiceConfig.username = config.getString("service.username");
        this.eagleServiceConfig.password = config.getString("service.password");
        this.eagleServiceConfig.readTimeoutSeconds = config.getInt("service.readTimeOutSeconds");
        this.eagleServiceConfig.maxFlushNum = 500;
        if (config.hasPath("service.maxFlushNum")) {
            this.eagleServiceConfig.maxFlushNum = config.getInt("service.maxFlushNum");
        }

        //parse job extractor
        this.jobExtractorConfig.site = config.getString("siteId");
        this.jobExtractorConfig.fetchRunningJobInterval = config.getInt("jobExtractorConfig.fetchRunningJobInterval");
        this.jobExtractorConfig.parseThreadPoolSize = config.getInt("jobExtractorConfig.parseThreadPoolSize");
        this.jobExtractorConfig.jobFetchSpoutParallism = config.getInt("jobExtractorConfig.numOfSpoutExecutors");
        this.jobExtractorConfig.jobFetchSpoutTasksNum = config.getInt("jobExtractorConfig.numOfSpoutTasks");
        this.jobExtractorConfig.jobParseBoltParallism = config.getInt("jobExtractorConfig.numOfParseBoltExecutors");
        this.jobExtractorConfig.jobParseBoltTasksNum = config.getInt("jobExtractorConfig.numOfParserBoltTasks");

        //parse endpointConfig config
        this.endpointConfig.rmUrls = config.getString("dataSourceConfig.rmUrls").split(",");
        this.endpointConfig.eventLog = config.getString("dataSourceConfig.hdfs.baseDir");
        for (Map.Entry<String, ConfigValue> entry : config.getConfig("dataSourceConfig.hdfs").entrySet()) {
            this.endpointConfig.hdfs.put(entry.getKey(), entry.getValue().unwrapped().toString());
        }

        LOG.info("Successfully initialized SparkRunningJobAppConfig");
        LOG.info("site: " + this.jobExtractorConfig.site);
        LOG.info("eagle.service.host: " + this.eagleServiceConfig.eagleServiceHost);
        LOG.info("eagle.service.port: " + this.eagleServiceConfig.eagleServicePort);
    }
}
