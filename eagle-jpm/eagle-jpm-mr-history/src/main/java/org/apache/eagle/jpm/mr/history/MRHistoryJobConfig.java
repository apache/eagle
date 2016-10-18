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

package org.apache.eagle.jpm.mr.history;

import com.typesafe.config.ConfigValue;
import org.apache.eagle.common.config.ConfigOptionParser;
import org.apache.eagle.jpm.util.DefaultJobIdPartitioner;
import org.apache.eagle.jpm.util.JobIdPartitioner;

import com.typesafe.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class MRHistoryJobConfig implements Serializable {
    private static final Logger LOG = LoggerFactory.getLogger(MRHistoryJobConfig.class);

    private static final String JOB_CONFIGURE_KEY_CONF_FILE = "JobConfigKeys.conf";

    public ZKStateConfig getZkStateConfig() {
        return zkStateConfig;
    }

    private ZKStateConfig zkStateConfig;

    public JobHistoryEndpointConfig getJobHistoryEndpointConfig() {
        return jobHistoryEndpointConfig;
    }

    private JobHistoryEndpointConfig jobHistoryEndpointConfig;

    public EagleServiceConfig getEagleServiceConfig() {
        return eagleServiceConfig;
    }

    private EagleServiceConfig eagleServiceConfig;

    public Config getConfig() {
        return config;
    }

    private Config config;

    public static class ZKStateConfig implements Serializable {
        public String zkQuorum;
        public String zkRoot;
        public int zkSessionTimeoutMs;
        public int zkRetryTimes;
        public int zkRetryInterval;
    }

    public static class JobHistoryEndpointConfig implements Serializable {
        public String mrHistoryServerUrl;
        public String basePath;
        public Map<String, String> hdfs;
        public String timeZone;
        public String site;
    }

    public static class EagleServiceConfig implements Serializable {
        public String eagleServiceHost;
        public int eagleServicePort;
        public String username;
        public String password;
        public int readTimeoutSeconds;
    }

    private static MRHistoryJobConfig manager = new MRHistoryJobConfig();

    /**
     * As this is singleton object and constructed while this class is being initialized,
     * so any exception within this constructor will be wrapped with java.lang.ExceptionInInitializerError.
     * And this is unrecoverable and hard to troubleshooting.
     */
    private MRHistoryJobConfig() {
        this.zkStateConfig = new ZKStateConfig();
        this.jobHistoryEndpointConfig = new JobHistoryEndpointConfig();
        this.jobHistoryEndpointConfig.hdfs = new HashMap<>();
        this.eagleServiceConfig = new EagleServiceConfig();
        this.config = null;
    }

    public static MRHistoryJobConfig getInstance(Config config) {
        if (config != null && manager.config == null) {
            manager.init(config);
        }

        return manager;
    }

    public static MRHistoryJobConfig get() {
        return getInstance(null);
    }

    /**
     * read configuration file and load hbase config etc.
     */
    private void init(Config config) {
        this.config = config;

        //parse eagle zk
        this.zkStateConfig.zkQuorum = config.getString("zookeeper.zkQuorum");
        this.zkStateConfig.zkSessionTimeoutMs = config.getInt("zookeeper.zkSessionTimeoutMs");
        this.zkStateConfig.zkRetryTimes = config.getInt("zookeeper.zkRetryTimes");
        this.zkStateConfig.zkRetryInterval = config.getInt("zookeeper.zkRetryInterval");
        this.zkStateConfig.zkRoot = config.getString("zookeeper.zkRoot");

        //parse job history endpoint
        this.jobHistoryEndpointConfig.site = config.getString("siteId");
        this.jobHistoryEndpointConfig.basePath = config.getString("endpointConfig.basePath");
        this.jobHistoryEndpointConfig.mrHistoryServerUrl = config.getString("endpointConfig.mrHistoryServerUrl");
        for (Map.Entry<String, ConfigValue> entry : config.getConfig("endpointConfig.hdfs").entrySet()) {
            this.jobHistoryEndpointConfig.hdfs.put(entry.getKey(), entry.getValue().unwrapped().toString());
        }
        this.jobHistoryEndpointConfig.timeZone = config.getString("endpointConfig.timeZone");

        // parse eagle service endpoint
        this.eagleServiceConfig.eagleServiceHost = config.getString("service.host");
        String port = config.getString("service.port");
        this.eagleServiceConfig.eagleServicePort = (port == null ? 8080 : Integer.parseInt(port));
        this.eagleServiceConfig.username = config.getString("service.username");
        this.eagleServiceConfig.password = config.getString("service.password");
        this.eagleServiceConfig.readTimeoutSeconds = config.getInt("service.readTimeOutSeconds");

        LOG.info("Successfully initialized MRHistoryJobConfig");
        LOG.info("zkStateConfig.zkQuorum: " + this.zkStateConfig.zkQuorum);
        LOG.info("eagleService.host: " + this.eagleServiceConfig.eagleServiceHost);
        LOG.info("eagleService.port: " + this.eagleServiceConfig.eagleServicePort);
    }
}
