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

    public static final String TOPOLOGY_DATA_FETCH_SPOUT_NAME = "topologyDataFetcherSpout";
    public static final String TOPOLOGY_ENTITY_PERSIST_BOLT_NAME = "topologyEntityPersistBolt";

    private static final int MAX_NUM_THREADS = 10;

    public DataExtractorConfig dataExtractorConfig;
    public HBaseConfig hBaseConfig;
    public HdfsConfig hdfsConfig;
    public MRConfig mrConfig;
    public List<TopologyConstants.TopologyType> topologyTypes;

    public Config config;

    private static TopologyCheckAppConfig configManager = new TopologyCheckAppConfig();

    private TopologyCheckAppConfig() {
        hBaseConfig = null;
        hdfsConfig = null;
        mrConfig = null;
        dataExtractorConfig = new DataExtractorConfig();
        topologyTypes = new ArrayList<>();
    }

    public static TopologyCheckAppConfig getInstance(Config config) {
        configManager.init(config);
        return configManager;
    }

    private void init(Config config) {
        this.config = config;

        this.dataExtractorConfig.site = config.getString("dataExtractorConfig.site");
        this.dataExtractorConfig.checkRetryTime = config.getLong("dataExtractorConfig.checkRetryTime");
        this.dataExtractorConfig.fetchDataIntervalInSecs = config.getLong("dataExtractorConfig.fetchDataIntervalInSecs");
        this.dataExtractorConfig.parseThreadPoolSize = MAX_NUM_THREADS;
        if (config.hasPath("dataExtractorConfig.parseThreadPoolSize")) {
            this.dataExtractorConfig.parseThreadPoolSize = config.getInt("dataExtractorConfig.parseThreadPoolSize");
        }
        this.dataExtractorConfig.numDataFetcherSpout = config.getInt("dataExtractorConfig.numDataFetcherSpout");
        this.dataExtractorConfig.numEntityPersistBolt = config.getInt("dataExtractorConfig.numEntityPersistBolt");


        if (config.hasPath("dataSourceConfig.hbase")) {
            topologyTypes.add(TopologyConstants.TopologyType.HBASE);
            hBaseConfig = new HBaseConfig();
            hBaseConfig.keytab = config.getString("dataSourceConfig.hbase.keytab");
            hBaseConfig.principal = config.getString("dataSourceConfig.hbase.principal");
            hBaseConfig.zkQuorum = config.getString("dataSourceConfig.hbase.zkQuorum");
            hBaseConfig.zkRoot = config.getString("dataSourceConfig.hbase.zkZnodeParent");
            hBaseConfig.zkClientPort = config.getString("dataSourceConfig.hbase.zkPropertyClientPort");
        }

        if (config.hasPath("dataSourceConfig.mr")) {
            topologyTypes.add(TopologyConstants.TopologyType.MR);
            mrConfig = new MRConfig();
            mrConfig.rmUrls =  config.getString("dataSourceConfig.mr.rmUrl").split(",\\s*");
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
        public int parseThreadPoolSize;
        public long checkRetryTime;
    }

    public static class HBaseConfig implements Serializable {
        public String zkQuorum;
        public String zkRoot;
        public String zkClientPort;
        public String principal;
        public String keytab;
    }

    public static class MRConfig implements Serializable {
        public String [] rmUrls;
    }

    public static class HdfsConfig implements Serializable {
        public String [] namenodeUrls;
    }
}
