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

package org.apache.eagle.jpm.mr.running;

import org.apache.eagle.common.config.ConfigOptionParser;

import com.typesafe.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;

public class MRRunningJobConfig implements Serializable {
    private static final Logger LOG = LoggerFactory.getLogger(MRRunningJobConfig.class);

    public ZKStateConfig getZkStateConfig() {
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
        public String zkPort;
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
        public int parseJobThreadPoolSize;
        public int topAndBottomTaskByElapsedTime;
    }

    public static class EndpointConfig implements Serializable {
        public String[] rmUrls;
    }

    public Config getConfig() {
        return config;
    }

    private Config config;

    private static MRRunningJobConfig manager = new MRRunningJobConfig();

    private MRRunningJobConfig() {
        this.eagleServiceConfig = new EagleServiceConfig();
        this.jobExtractorConfig = new JobExtractorConfig();
        this.endpointConfig = new EndpointConfig();
        this.zkStateConfig = new ZKStateConfig();
    }

    public static MRRunningJobConfig getInstance(String[] args) {
        try {
            LOG.info("Loading from configuration file");
            return getInstance(new ConfigOptionParser().load(args));
        } catch (Exception e) {
            LOG.error("failed to load config");
            throw new IllegalArgumentException("Failed to load config", e);
        }
    }

    public static MRRunningJobConfig getInstance(Config config) {
        manager.init(config);
        return manager;
    }

    private void init(Config config) {
        this.config = config;

        //parse eagle zk
        this.zkStateConfig.zkQuorum = config.getString("zookeeperConfig.zkQuorum");
        this.zkStateConfig.zkPort = config.getString("zookeeperConfig.zkPort");
        this.zkStateConfig.zkSessionTimeoutMs = config.getInt("zookeeperConfig.zkSessionTimeoutMs");
        this.zkStateConfig.zkRetryTimes = config.getInt("zookeeperConfig.zkRetryTimes");
        this.zkStateConfig.zkRetryInterval = config.getInt("zookeeperConfig.zkRetryInterval");
        this.zkStateConfig.zkRoot = config.getString("zookeeperConfig.zkRoot");

        // parse eagle service endpoint
        this.eagleServiceConfig.eagleServiceHost = config.getString("eagleProps.eagleService.host");
        String port = config.getString("eagleProps.eagleService.port");
        this.eagleServiceConfig.eagleServicePort = Integer.parseInt(port);
        this.eagleServiceConfig.username = config.getString("eagleProps.eagleService.username");
        this.eagleServiceConfig.password = config.getString("eagleProps.eagleService.password");
        this.eagleServiceConfig.readTimeoutSeconds = config.getInt("eagleProps.eagleService.readTimeOutSeconds");
        this.eagleServiceConfig.maxFlushNum = config.getInt("eagleProps.eagleService.maxFlushNum");
        //parse job extractor
        this.jobExtractorConfig.site = config.getString("jobExtractorConfig.site");
        this.jobExtractorConfig.fetchRunningJobInterval = config.getInt("jobExtractorConfig.fetchRunningJobInterval");
        this.jobExtractorConfig.parseJobThreadPoolSize = config.getInt("jobExtractorConfig.parseJobThreadPoolSize");
        this.jobExtractorConfig.topAndBottomTaskByElapsedTime = config.getInt("jobExtractorConfig.topAndBottomTaskByElapsedTime");

        //parse data source config
        this.endpointConfig.rmUrls = config.getString("dataSourceConfig.rmUrls").split(",");

        LOG.info("Successfully initialized MRRunningJobConfig");
        LOG.info("site: " + this.jobExtractorConfig.site);
        LOG.info("eagle.service.host: " + this.eagleServiceConfig.eagleServiceHost);
        LOG.info("eagle.service.port: " + this.eagleServiceConfig.eagleServicePort);
    }
}
