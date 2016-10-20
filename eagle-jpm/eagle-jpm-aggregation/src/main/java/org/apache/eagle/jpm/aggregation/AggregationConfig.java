/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.eagle.jpm.aggregation;

import com.typesafe.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;

public class AggregationConfig implements Serializable {
    private static final Logger LOG = LoggerFactory.getLogger(AggregationConfig.class);

    private static final String ZK_ROOT_PREFIX = "/apps/mr/aggregation";

    public ZKStateConfig getZkStateConfig() {
        return zkStateConfig;
    }

    private ZKStateConfig zkStateConfig;

    public EagleServiceConfig getEagleServiceConfig() {
        return eagleServiceConfig;
    }

    private EagleServiceConfig eagleServiceConfig;

    public StormConfig getStormConfig() {
        return stormConfig;
    }

    private StormConfig stormConfig;

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

    public static class StormConfig implements Serializable {
        public String site;
        public long aggregationDuration;
    }

    public static class EagleServiceConfig implements Serializable {
        public String eagleServiceHost;
        public int eagleServicePort;
        public String username;
        public String password;
    }

    private static AggregationConfig manager = new AggregationConfig();

    private AggregationConfig() {
        this.zkStateConfig = new ZKStateConfig();
        this.stormConfig = new StormConfig();
        this.eagleServiceConfig = new EagleServiceConfig();
        this.config = null;
    }

    public static AggregationConfig getInstance(Config config) {
        if (config != null && manager.config == null) {
            manager.init(config);
        }

        return manager;
    }

    public static AggregationConfig get() {
        return getInstance(null);
    }

    /**
     * read configuration file and load hbase config etc.
     */
    private void init(Config config) {
        this.config = config;
        //parse stormConfig
        this.stormConfig.site = config.getString("siteId");
        this.stormConfig.aggregationDuration = config.getLong("stormConfig.aggregationDuration");

        //parse eagle zk
        this.zkStateConfig.zkQuorum = config.getString("zookeeper.zkQuorum");
        this.zkStateConfig.zkSessionTimeoutMs = config.getInt("zookeeper.zkSessionTimeoutMs");
        this.zkStateConfig.zkRetryTimes = config.getInt("zookeeper.zkRetryTimes");
        this.zkStateConfig.zkRetryInterval = config.getInt("zookeeper.zkRetryInterval");
        this.zkStateConfig.zkRoot = ZK_ROOT_PREFIX + "/" + this.stormConfig.site;

        // parse eagle service endpoint
        this.eagleServiceConfig.eagleServiceHost = config.getString("service.host");
        String port = config.getString("service.port");
        this.eagleServiceConfig.eagleServicePort = (port == null ? 8080 : Integer.parseInt(port));
        this.eagleServiceConfig.username = config.getString("service.username");
        this.eagleServiceConfig.password = config.getString("service.password");

        LOG.info("Successfully initialized Aggregation Config");
        LOG.info("zookeeper.quorum: " + this.zkStateConfig.zkQuorum);
        LOG.info("service.host: " + this.eagleServiceConfig.eagleServiceHost);
        LOG.info("service.port: " + this.eagleServiceConfig.eagleServicePort);
    }
}
