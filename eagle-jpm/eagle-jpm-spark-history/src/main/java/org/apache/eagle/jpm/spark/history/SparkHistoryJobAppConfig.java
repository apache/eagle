/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one or more
 *  * contributor license agreements.  See the NOTICE file distributed with
 *  * this work for additional information regarding copyright ownership.
 *  * The ASF licenses this file to You under the Apache License, Version 2.0
 *  * (the "License"); you may not use this file except in compliance with
 *  * the License.  You may obtain a copy of the License at
 *  *
 *  *    http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package org.apache.eagle.jpm.spark.history;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigValue;
import org.apache.eagle.service.client.impl.EagleServiceBaseClient;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class SparkHistoryJobAppConfig implements Serializable {
    static final String SPARK_HISTORY_JOB_FETCH_SPOUT_NAME = "sparkHistoryJobFetchSpout";
    static final String SPARK_HISTORY_JOB_PARSE_BOLT_NAME = "sparkHistoryJobParseBolt";

    static final String DEFAULT_SPARK_JOB_HISTORY_ZOOKEEPER_ROOT = "/eagle/sparkJobHistory";

    public ZKStateConfig zkStateConfig;
    public JobHistoryEndpointConfig jobHistoryConfig;
    public EagleInfo eagleInfo;
    public StormConfig stormConfig;

    private Config config;

    public Config getConfig() {
        return config;
    }

    private SparkHistoryJobAppConfig(Config config) {
        this.zkStateConfig = new ZKStateConfig();
        this.jobHistoryConfig = new JobHistoryEndpointConfig();
        this.jobHistoryConfig.hdfs = new HashMap<>();
        this.eagleInfo = new EagleInfo();
        this.stormConfig = new StormConfig();
        init(config);
    }

    public static SparkHistoryJobAppConfig newInstance(Config config) {
        return new SparkHistoryJobAppConfig(config);
    }

    private void init(Config config) {
        this.config = config;

        this.zkStateConfig.zkQuorum = config.getString("zookeeper.zkQuorum");
        this.zkStateConfig.zkRetryInterval = config.getInt("zookeeper.zkRetryInterval");
        this.zkStateConfig.zkRetryTimes = config.getInt("zookeeper.zkRetryTimes");
        this.zkStateConfig.zkSessionTimeoutMs = config.getInt("zookeeper.zkSessionTimeoutMs");
        this.zkStateConfig.zkRoot = DEFAULT_SPARK_JOB_HISTORY_ZOOKEEPER_ROOT;
        if (config.hasPath("zookeeper.zkRoot")) {
            this.zkStateConfig.zkRoot = config.getString("zookeeper.zkRoot");
        }

        jobHistoryConfig.rms = config.getString("dataSourceConfig.rm.url").split(",\\s*");
        jobHistoryConfig.baseDir = config.getString("dataSourceConfig.baseDir");
        for (Map.Entry<String, ConfigValue> entry : config.getConfig("dataSourceConfig.hdfs").entrySet()) {
            this.jobHistoryConfig.hdfs.put(entry.getKey(), entry.getValue().unwrapped().toString());
        }

        this.eagleInfo.host = config.getString("service.host");
        this.eagleInfo.port = config.getInt("service.port");
        this.eagleInfo.username = config.getString("service.username");
        this.eagleInfo.password = config.getString("service.password");
        this.eagleInfo.timeout = 2;
        if (config.hasPath("service.readTimeOutSeconds")) {
            this.eagleInfo.timeout = config.getInt("service.readTimeOutSeconds");
        }
        this.eagleInfo.basePath = EagleServiceBaseClient.DEFAULT_BASE_PATH;
        if (config.hasPath("service.basePath")) {
            this.eagleInfo.basePath = config.getString("service.basePath");
        }

        this.stormConfig.siteId = config.getString("siteId");
        this.stormConfig.spoutCrawlInterval = config.getInt("topology.spoutCrawlInterval");
        this.stormConfig.numOfSpoutExecutors = config.getInt("topology.numOfSpoutExecutors");
        this.stormConfig.numOfSpoutTasks = config.getInt("topology.numOfSpoutTasks");
        this.stormConfig.numOfParserBoltExecutors = config.getInt("topology.numOfParseBoltExecutors");
        this.stormConfig.numOfParserBoltTasks = config.getInt("topology.numOfParserBoltTasks");
    }

    public static class ZKStateConfig implements Serializable {
        public String zkQuorum;
        public String zkRoot;
        public int zkSessionTimeoutMs;
        public int zkRetryTimes;
        public int zkRetryInterval;
    }

    public static class JobHistoryEndpointConfig implements Serializable {
        public String[] rms;
        public String baseDir;
        public Map<String, String> hdfs;
    }

    public static class StormConfig implements Serializable {
        public String siteId;
        public int spoutCrawlInterval;
        public int numOfSpoutExecutors;
        public int numOfSpoutTasks;
        public int numOfParserBoltExecutors;
        public int numOfParserBoltTasks;
    }

    public static class EagleInfo implements Serializable {
        public String host;
        public int port;
        public String username;
        public String password;
        public String basePath;
        public int timeout;
    }
}
