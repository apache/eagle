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

import org.apache.eagle.common.config.ConfigOptionParser;
import org.apache.eagle.jpm.util.DefaultJobIdPartitioner;
import org.apache.eagle.jpm.util.JobIdPartitioner;

import com.typesafe.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;

public class MRHistoryJobConfig implements Serializable {
    private static final Logger LOG = LoggerFactory.getLogger(MRHistoryJobConfig.class);

    private static final String JOB_CONFIGURE_KEY_CONF_FILE = "JobConfigKeys.conf";

    public String getEnv() {
        return env;
    }

    private String env;

    public ZKStateConfig getZkStateConfig() {
        return zkStateConfig;
    }

    private ZKStateConfig zkStateConfig;

    public JobHistoryEndpointConfig getJobHistoryEndpointConfig() {
        return jobHistoryEndpointConfig;
    }

    private JobHistoryEndpointConfig jobHistoryEndpointConfig;

    public ControlConfig getControlConfig() {
        return controlConfig;
    }

    private ControlConfig controlConfig;

    public JobExtractorConfig getJobExtractorConfig() {
        return jobExtractorConfig;
    }

    private JobExtractorConfig jobExtractorConfig;

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
        public String zkPort;
    }

    public static class JobHistoryEndpointConfig implements Serializable {
        public String nnEndpoint;
        public String basePath;
        public boolean pathContainsJobTrackerName;
        public String jobTrackerName;
        public String principal;
        public String keyTab;
    }

    public static class ControlConfig implements Serializable {
        public boolean dryRun;
        public Class<? extends JobIdPartitioner> partitionerCls;
        public boolean zeroBasedMonth;
        public String timeZone;
    }

    public static class JobExtractorConfig implements Serializable {
        public String site;
        public String mrVersion;
        public int readTimeoutSeconds;
    }

    public static class EagleServiceConfig implements Serializable {
        public String eagleServiceHost;
        public int eagleServicePort;
        public String username;
        public String password;
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
        this.controlConfig = new ControlConfig();
        this.jobExtractorConfig = new JobExtractorConfig();
        this.eagleServiceConfig = new EagleServiceConfig();
    }

    public static MRHistoryJobConfig getInstance(String[] args) {
        manager.init(args);
        return manager;
    }

    public static MRHistoryJobConfig getInstance(Config config) {
        manager.init(config);
        return manager;
    }

    /**
     * read configuration file and load hbase config etc.
     */
    private void init(String[] args) {
        // TODO: Probably we can remove the properties file path check in future
        try {
            LOG.info("Loading from configuration file");
            init(new ConfigOptionParser().load(args));
        } catch (Exception e) {
            LOG.error("failed to load config");
        }
    }

    /**
     * read configuration file and load hbase config etc.
     */
    private void init(Config config) {
        this.config = config;
        this.env = config.getString("envContextConfig.env");
        //parse eagle job extractor
        this.jobExtractorConfig.site = config.getString("jobExtractorConfig.site");
        this.jobExtractorConfig.mrVersion = config.getString("jobExtractorConfig.mrVersion");
        this.jobExtractorConfig.readTimeoutSeconds = config.getInt("jobExtractorConfig.readTimeOutSeconds");
        //parse eagle zk
        this.zkStateConfig.zkQuorum = config.getString("dataSourceConfig.zkQuorum");
        this.zkStateConfig.zkPort = config.getString("dataSourceConfig.zkPort");
        this.zkStateConfig.zkSessionTimeoutMs = config.getInt("dataSourceConfig.zkSessionTimeoutMs");
        this.zkStateConfig.zkRetryTimes = config.getInt("dataSourceConfig.zkRetryTimes");
        this.zkStateConfig.zkRetryInterval = config.getInt("dataSourceConfig.zkRetryInterval");
        this.zkStateConfig.zkRoot = config.getString("dataSourceConfig.zkRoot");

        //parse job history endpoint
        this.jobHistoryEndpointConfig.basePath = config.getString("dataSourceConfig.basePath");
        this.jobHistoryEndpointConfig.jobTrackerName = config.getString("dataSourceConfig.jobTrackerName");
        this.jobHistoryEndpointConfig.nnEndpoint = config.getString("dataSourceConfig.nnEndpoint");
        this.jobHistoryEndpointConfig.pathContainsJobTrackerName = config.getBoolean("dataSourceConfig.pathContainsJobTrackerName");
        this.jobHistoryEndpointConfig.principal = config.getString("dataSourceConfig.principal");
        this.jobHistoryEndpointConfig.keyTab = config.getString("dataSourceConfig.keytab");

        //parse control config
        this.controlConfig.dryRun = config.getBoolean("dataSourceConfig.dryRun");
        try {
            this.controlConfig.partitionerCls = (Class<? extends JobIdPartitioner>) Class.forName(config.getString("dataSourceConfig.partitionerCls"));
            assert this.controlConfig.partitionerCls != null;
        } catch (Exception e) {
            LOG.warn("can not initialize partitioner class, use org.apache.eagle.jpm.util.DefaultJobIdPartitioner", e);
            this.controlConfig.partitionerCls = DefaultJobIdPartitioner.class;
        } finally {
            LOG.info("Loaded partitioner class: {}", this.controlConfig.partitionerCls);
        }
        this.controlConfig.zeroBasedMonth = config.getBoolean("dataSourceConfig.zeroBasedMonth");
        this.controlConfig.timeZone = config.getString("dataSourceConfig.timeZone");

        // parse eagle service endpoint
        this.eagleServiceConfig.eagleServiceHost = config.getString("eagleProps.eagleService.host");
        String port = config.getString("eagleProps.eagleService.port");
        this.eagleServiceConfig.eagleServicePort = (port == null ? 8080 : Integer.parseInt(port));
        this.eagleServiceConfig.username = config.getString("eagleProps.eagleService.username");
        this.eagleServiceConfig.password = config.getString("eagleProps.eagleService.password");

        LOG.info("Successfully initialized MRHistoryJobConfig");
        LOG.info("env: " + this.env);
        LOG.info("zookeeper.quorum: " + this.zkStateConfig.zkQuorum);
        LOG.info("zookeeper.property.clientPort: " + this.zkStateConfig.zkPort);
        LOG.info("eagle.service.host: " + this.eagleServiceConfig.eagleServiceHost);
        LOG.info("eagle.service.port: " + this.eagleServiceConfig.eagleServicePort);
    }
}
