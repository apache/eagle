/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

package org.apache.eagle.topology;

import com.typesafe.config.Config;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class TopologyCheckAppConfig implements Serializable {

    final static String TOPOLOGY_DATA_FETCH_SPOUT_NAME = "topologyDataFetcherSpout";
    final static String TOPOLOGY_ENTITY_PERSIST_BOLT_NAME = "topologyEntityPersistBolt";

    public TopologyConfig topologyConfig;
    public HBaseConfig hBaseConfig;
    public HdfsConfig hdfsConfig;
    public MRConfig mrConfig;
    public EagleInfo eagleInfo;
    public List<String> topologyTypes;

    public Config config;

    private static TopologyCheckAppConfig configManager = new TopologyCheckAppConfig();

    private TopologyCheckAppConfig() {
        hBaseConfig = null;
        hdfsConfig = null;
        mrConfig = null;
        eagleInfo = new EagleInfo();
        topologyConfig = new TopologyConfig();
        topologyTypes = new ArrayList<>();
    }

    public static TopologyCheckAppConfig getInstance(Config config) {
        configManager.init(config);
        return configManager;
    }

    private void init(Config config) {
        this.config = config;

        this.eagleInfo.host = config.getString("eagleProps.eagle.service.host");
        this.eagleInfo.port = config.getInt("eagleProps.eagle.service.port");
        this.eagleInfo.password = config.getString("eagleProps.eagle.service.password");
        this.eagleInfo.username = config.getString("eagleProps.eagle.service.username");

        this.topologyConfig.site = config.getString("dataSourceConfig.site");
        this.topologyConfig.checkRetryTime = config.getLong("dataSourceConfig.checkRetryTime");
        this.topologyConfig.fetchDataIntervalInSecs = config.getLong("dataSourceConfig.fetchDataIntervalInSecs");
        this.topologyConfig.parseThreadPoolSize = config.getInt("dataSourceConfig.parseThreadPoolSize");
        this.topologyConfig.numDataFetcherSpout = config.getInt("parallelismConfig.topologyDataFetcherSpout");
        this.topologyConfig.numEntityPersistBolt = config.getInt("parallelismConfig.topologyEntityPersistBolt");


        if (config.hasPath("dataSourceConfig.hbase")) {
            topologyTypes.add(TopologyConstants.TopologyType.HBASE.toString());
            hBaseConfig = new HBaseConfig();
            hBaseConfig.keytab = config.getString("dataSourceConfig.hbase.keytab");
            hBaseConfig.principal = config.getString("dataSourceConfig.hbase.principal");
            hBaseConfig.zkQuorum = config.getString("dataSourceConfig.hbase.zkQuorum");
            hBaseConfig.zkRoot = config.getString("dataSourceConfig.hbase.zkRoot");
            hBaseConfig.zkRetryInterval = config.getInt("dataSourceConfig.hbase.zkRetryInterval");
            hBaseConfig.zkRetryTimes = config.getInt("dataSourceConfig.hbase.zkRetryTimes");
            hBaseConfig.zkSessionTimeoutMs = config.getInt("dataSourceConfig.hbase.zkSessionTimeoutMs");
        }

        if (config.hasPath("dataSourceConfig.mr")) {
            topologyTypes.add(TopologyConstants.TopologyType.MR.toString());
            mrConfig = new MRConfig();
            mrConfig.rmUrls =  config.getString("dataSourceConfig.yarn.rmUrl").split(",\\s*");
        }

        if (config.hasPath("dataSourceConfig.hdfs")) {
            topologyTypes.add(TopologyConstants.TopologyType.HDFS.toString());
            hdfsConfig = new HdfsConfig();
            hdfsConfig.namenodeUrls = config.getString("dataSourceConfig.hdfs.namenodeUrl").split(",\\s*");
        }
    }

    public static class TopologyConfig implements Serializable {
        public String site;
        public int numDataFetcherSpout;
        public int numEntityPersistBolt;
        public long fetchDataIntervalInSecs;
        public long parseThreadPoolSize;
        public long checkRetryTime;
    }

    public static class HBaseConfig implements Serializable {
        public String zkQuorum;
        public String zkRoot;
        public int zkSessionTimeoutMs;
        public int zkRetryTimes;
        public int zkRetryInterval;
        public String principal;
        public String keytab;
    }

    public static class MRConfig implements Serializable {
        public String [] rmUrls;
    }

    public static class HdfsConfig implements Serializable {
        public String [] namenodeUrls;
    }

    public static class EagleInfo implements Serializable {
        public String host;
        public int port;
        public String username;
        public String password;
        public long readTimeOutSeconds;
    }
}
