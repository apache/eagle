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

    public DataExtractorConfig dataExtractorConfig;
    public HBaseConfig hBaseConfig;
    public HdfsConfig hdfsConfig;
    public MRConfig mrConfig;
    public EagleInfo eagleInfo;
    public List<TopologyConstants.TopologyType> topologyTypes;

    public Config config;

    private static TopologyCheckAppConfig configManager = new TopologyCheckAppConfig();

    private TopologyCheckAppConfig() {
        hBaseConfig = null;
        hdfsConfig = null;
        mrConfig = null;
        eagleInfo = new EagleInfo();
        dataExtractorConfig = new DataExtractorConfig();
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

        this.dataExtractorConfig.site = config.getString("dataExtractorConfig.site");
        this.dataExtractorConfig.checkRetryTime = config.getLong("dataExtractorConfig.checkRetryTime");
        this.dataExtractorConfig.fetchDataIntervalInSecs = config.getLong("dataExtractorConfig.fetchDataIntervalInSecs");
        this.dataExtractorConfig.parseThreadPoolSize = config.getInt("dataExtractorConfig.parseThreadPoolSize");
        this.dataExtractorConfig.numDataFetcherSpout = config.getInt("dataExtractorConfig.numDataFetcherSpout");
        this.dataExtractorConfig.numEntityPersistBolt = config.getInt("dataExtractorConfig.numEntityPersistBolt");


        if (config.hasPath("dataSourceConfig.hbase")) {
            topologyTypes.add(TopologyConstants.TopologyType.HBASE);
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
            topologyTypes.add(TopologyConstants.TopologyType.MR);
            mrConfig = new MRConfig();
            mrConfig.rmUrls =  config.getString("dataSourceConfig.yarn.rmUrl").split(",\\s*");
        }

        if (config.hasPath("dataSourceConfig.hdfs")) {
            topologyTypes.add(TopologyConstants.TopologyType.HDFS);
            hdfsConfig = new HdfsConfig();
            hdfsConfig.namenodeUrls = config.getString("dataSourceConfig.hdfs.namenodeUrl").split(",\\s*");
        }
    }

    public static class DataExtractorConfig implements Serializable {
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
