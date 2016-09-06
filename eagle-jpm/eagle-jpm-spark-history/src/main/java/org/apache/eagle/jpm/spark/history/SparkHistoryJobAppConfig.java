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
import com.typesafe.config.ConfigFactory;

import java.io.Serializable;

public class SparkHistoryJobAppConfig implements Serializable {
    final static String SPARK_HISTORY_JOB_FETCH_SPOUT_NAME = "sparkHistoryJobFetchSpout";
    final static String SPARK_HISTORY_JOB_PARSE_BOLT_NAME = "sparkHistoryJobParseBolt";

    public ZKStateConfig zkStateConfig;
    public JobHistoryEndpointConfig jobHistoryConfig;
    public HDFSConfig hdfsConfig;
    public BasicInfo info;
    public EagleInfo eagleInfo;
    public StormConfig stormConfig;

    private Config config;

    private static SparkHistoryJobAppConfig manager = new SparkHistoryJobAppConfig();
    
    public Config getConfig() {
        return config;
    }

    public SparkHistoryJobAppConfig() {
        this.zkStateConfig = new ZKStateConfig();
        this.jobHistoryConfig = new JobHistoryEndpointConfig();
        this.hdfsConfig = new HDFSConfig();
        this.info = new BasicInfo();
        this.eagleInfo = new EagleInfo();
        this.stormConfig = new StormConfig();
    }

    public static SparkHistoryJobAppConfig getInstance(Config config) {
        manager.init(config);
        return manager;
    }

    private void init(Config config) {
        this.config = config;

        this.zkStateConfig.zkQuorum = config.getString("dataSourceConfig.zkQuorum");
        this.zkStateConfig.zkRetryInterval = config.getInt("dataSourceConfig.zkRetryInterval");
        this.zkStateConfig.zkRetryTimes = config.getInt("dataSourceConfig.zkRetryTimes");
        this.zkStateConfig.zkSessionTimeoutMs = config.getInt("dataSourceConfig.zkSessionTimeoutMs");
        this.zkStateConfig.zkRoot = config.getString("dataSourceConfig.zkRoot");

        jobHistoryConfig.historyServerUrl = config.getString("dataSourceConfig.spark.history.server.url");
        jobHistoryConfig.historyServerUserName = config.getString("dataSourceConfig.spark.history.server.username");
        jobHistoryConfig.historyServerUserPwd = config.getString("dataSourceConfig.spark.history.server.password");
        jobHistoryConfig.rms = config.getString("dataSourceConfig.rm.url").split(",\\s*");

        this.hdfsConfig.baseDir = config.getString("dataSourceConfig.hdfs.eventLog");
        this.hdfsConfig.endpoint = config.getString("dataSourceConfig.hdfs.endPoint");
        this.hdfsConfig.principal = config.getString("dataSourceConfig.hdfs.principal");
        this.hdfsConfig.keytab = config.getString("dataSourceConfig.hdfs.keytab");

        info.site = config.getString("basic.cluster") + "-" + config.getString("basic.dataCenter");
        info.jobConf = config.getString("basic.jobConf.additional.info").split(",\\s*");

        this.eagleInfo.host = config.getString("eagleProps.eagle.service.host");
        this.eagleInfo.port = config.getInt("eagleProps.eagle.service.port");

        this.stormConfig.timeoutSec = config.getInt("storm.messageTimeoutSec");
        this.stormConfig.spoutPending = config.getInt("storm.pendingSpout");
        this.stormConfig.spoutCrawlInterval = config.getInt("storm.spoutCrawlInterval");
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
        public String historyServerUrl;
        public String historyServerUserName;
        public String historyServerUserPwd;
    }

    public static class HDFSConfig implements Serializable {
        public String endpoint;
        public String baseDir;
        public String principal;
        public String keytab;
    }

    public static class BasicInfo implements Serializable {
        public String site;
        public String[] jobConf;
    }

    public static class StormConfig implements Serializable {
        public int timeoutSec;
        public int spoutPending;
        public int spoutCrawlInterval;
    }

    public static class EagleInfo implements Serializable {
        public String host;
        public int port;
    }
}
