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

    private static final String ZK_ROOT_PREFIX = "/apps/mr/running";

    public ZKStateConfig getZkStateConfig() {
        return zkStateConfig;
    }

    private ZKStateConfig zkStateConfig;

    public EagleServiceConfig getEagleServiceConfig() {
        return eagleServiceConfig;
    }

    private EagleServiceConfig eagleServiceConfig;

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
    }

    public static class EagleServiceConfig implements Serializable {
        public String eagleServiceHost;
        public int eagleServicePort;
        public int readTimeoutSeconds;
        public String username;
        public String password;
    }

    public static class EndpointConfig implements Serializable {
        public String site;
        public String[] rmUrls;
        public int fetchRunningJobInterval;
        public int parseJobThreadPoolSize;
    }

    public Config getConfig() {
        return config;
    }

    private Config config;

    private MRRunningJobConfig(Config config) {
        this.eagleServiceConfig = new EagleServiceConfig();
        this.endpointConfig = new EndpointConfig();
        this.zkStateConfig = new ZKStateConfig();
        init(config);
    }

    public static MRRunningJobConfig newInstance(String[] args) {
        try {
            LOG.info("Loading from configuration file");
            return newInstance(new ConfigOptionParser().load(args));
        } catch (Exception e) {
            LOG.error("failed to load config");
            throw new IllegalArgumentException("Failed to load config", e);
        }
    }

    public static MRRunningJobConfig newInstance(Config config) {
        return new MRRunningJobConfig(config);
    }

    private void init(Config config) {
        this.config = config;

        //parse eagle zk
        this.zkStateConfig.zkQuorum = config.getString("zookeeper.zkQuorum");
        this.zkStateConfig.zkSessionTimeoutMs = config.getInt("zookeeper.zkSessionTimeoutMs");
        this.zkStateConfig.zkRetryTimes = config.getInt("zookeeper.zkRetryTimes");
        this.zkStateConfig.zkRetryInterval = config.getInt("zookeeper.zkRetryInterval");
        this.zkStateConfig.zkRoot = ZK_ROOT_PREFIX + "/" + config.getString("siteId");

        // parse eagle service endpoint
        this.eagleServiceConfig.eagleServiceHost = config.getString("service.host");
        String port = config.getString("service.port");
        this.eagleServiceConfig.eagleServicePort = Integer.parseInt(port);
        this.eagleServiceConfig.username = config.getString("service.username");
        this.eagleServiceConfig.password = config.getString("service.password");
        this.eagleServiceConfig.readTimeoutSeconds = config.getInt("service.readTimeOutSeconds");

        //parse data source config
        this.endpointConfig.rmUrls = config.getString("endpointConfig.rmUrls").split(",");
        this.endpointConfig.site = config.getString("siteId");
        this.endpointConfig.fetchRunningJobInterval = config.getInt("endpointConfig.fetchRunningJobInterval");
        this.endpointConfig.parseJobThreadPoolSize = config.getInt("endpointConfig.parseJobThreadPoolSize");

        LOG.info("Successfully initialized MRRunningJobConfig");
        LOG.info("site: " + this.endpointConfig.site);
        LOG.info("eagle.service.host: " + this.eagleServiceConfig.eagleServiceHost);
        LOG.info("eagle.service.port: " + this.eagleServiceConfig.eagleServicePort);
    }
}
