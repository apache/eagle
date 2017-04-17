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
import org.apache.eagle.topology.resolver.TopologyRackResolver;
import org.apache.eagle.topology.resolver.impl.DefaultTopologyRackResolver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class TopologyCheckAppConfig implements Serializable {

    public static final String TOPOLOGY_DATA_FETCH_SPOUT_NAME = "topologyDataFetcherSpout";
    public static final String SYSTEM_DATA_FETCH_SPOUT_NAME = "systemDataFetcherSpout";
    public static final String SYSTEM_ENTITY_PERSIST_BOLT_NAME = "systemEntityPersistBolt";
    public static final String TOPOLOGY_ENTITY_PERSIST_BOLT_NAME = "topologyEntityPersistBolt";
    public static final String PARSE_BOLT_NAME = "parserBolt";
    public static final String SINK_BOLT_NAME = "sinkBolt";

    private static final int MAX_NUM_THREADS = 10;
    private static final String HBASE_ZOOKEEPER_CLIENT_PORT = "2181";

    private static final Logger LOG = LoggerFactory.getLogger(TopologyCheckAppConfig.class);

    public DataExtractorConfig dataExtractorConfig;
    public HBaseConfig hBaseConfig;
    public HdfsConfig hdfsConfig;
    public MRConfig mrConfig;
    public SystemConfig systemConfig;
    public List<TopologyConstants.TopologyType> topologyTypes;

    private Config config;

    private TopologyCheckAppConfig(Config config) {
        hBaseConfig = null;
        hdfsConfig = null;
        mrConfig = null;
        dataExtractorConfig = new DataExtractorConfig();
        topologyTypes = new ArrayList<>();
        init(config);
    }

    public Config getConfig() {
        return config;
    }

    public static TopologyCheckAppConfig newInstance(Config config) {
        return new TopologyCheckAppConfig(config);
    }

    private void init(Config config) {
        this.config = config;

        this.dataExtractorConfig.site = config.getString("siteId");
        this.dataExtractorConfig.fetchDataIntervalInSecs = config.getLong("topology.fetchDataIntervalInSecs");
        this.dataExtractorConfig.parseThreadPoolSize = MAX_NUM_THREADS;
        if (config.hasPath("topology.parseThreadPoolSize")) {
            this.dataExtractorConfig.parseThreadPoolSize = config.getInt("topology.parseThreadPoolSize");
        }
        this.dataExtractorConfig.numDataFetcherSpout = config.getInt("topology.numDataFetcherSpout");
        this.dataExtractorConfig.numEntityPersistBolt = config.getInt("topology.numEntityPersistBolt");
        this.dataExtractorConfig.numKafkaSinkBolt = config.getInt("topology.numOfKafkaSinkBolt");

        this.dataExtractorConfig.resolverCls = DefaultTopologyRackResolver.class;
        if (config.hasPath("topology.rackResolverCls")) {
            String resolveCls = config.getString("topology.rackResolverCls");
            try {
                this.dataExtractorConfig.resolverCls = (Class<? extends TopologyRackResolver>) Class.forName(resolveCls);
            } catch (ClassNotFoundException e) {
                LOG.warn("{} is not found, will use DefaultTopologyRackResolver instead", resolveCls);
            }
        }

        if (config.hasPath("dataSourceConfig.hbase.enabled") && config.getBoolean("dataSourceConfig.hbase.enabled")) {
            topologyTypes.add(TopologyConstants.TopologyType.HBASE);
            hBaseConfig = new HBaseConfig();

            hBaseConfig.zkQuorum = config.getString("dataSourceConfig.hbase.zkQuorum");
            hBaseConfig.zkRoot = config.getString("dataSourceConfig.hbase.zkZnodeParent");
            hBaseConfig.zkClientPort = getOptionalConfig("dataSourceConfig.hbase.zkPropertyClientPort", HBASE_ZOOKEEPER_CLIENT_PORT);
            hBaseConfig.zkRetryTimes = getOptionalConfig("dataSourceConfig.hbase.zkRetryTimes", "5");
            hBaseConfig.eagleKeytab = getOptionalConfig("dataSourceConfig.hbase.kerberos.eagle.keytab", null);
            hBaseConfig.eaglePrincipal = getOptionalConfig("dataSourceConfig.hbase.kerberos.eagle.principal", null);
            hBaseConfig.hbaseMasterPrincipal = getOptionalConfig("dataSourceConfig.hbase.kerberos.master.principal", null);
        }

        if (config.hasPath("dataSourceConfig.mr.enabled") && config.getBoolean("dataSourceConfig.mr.enabled")) {
            topologyTypes.add(TopologyConstants.TopologyType.MR);
            mrConfig = new MRConfig();
            mrConfig.rmUrls = config.getString("dataSourceConfig.mr.rmUrl").split(",\\s*");
            mrConfig.historyServerUrl = getOptionalConfig("dataSourceConfig.mr.historyServerUrl", null);
        }

        if (config.hasPath("dataSourceConfig.hdfs.enabled") && config.getBoolean("dataSourceConfig.hdfs.enabled")) {
            topologyTypes.add(TopologyConstants.TopologyType.HDFS);
            hdfsConfig = new HdfsConfig();
            hdfsConfig.namenodeUrls = config.getString("dataSourceConfig.hdfs.namenodeUrl").split(",\\s*");
        }

        if (config.hasPath("dataSourceConfig.system.enabled") && config.getBoolean("dataSourceConfig.system.enabled")) {
            systemConfig = new SystemConfig();
            systemConfig.systemInstanceKafkaTopic = getOptionalConfig("dataSourceConfig.system.topic", null);
            systemConfig.systemInstanceKafkaSchemeCls = getOptionalConfig("dataSourceConfig.system.schemeCls", null);
            systemConfig.systemInstanceZkQuorum = getOptionalConfig("dataSourceConfig.system.zkConnection", null);
            systemConfig.systemInstanceSendBatchSize = config.getInt("dataSourceConfig.system.dataSendBatchSize");
        }
    }

    public static class DataExtractorConfig implements Serializable {
        public String site;
        public int numDataFetcherSpout;
        public int numEntityPersistBolt;
        public int numKafkaSinkBolt;
        public long fetchDataIntervalInSecs;
        public int parseThreadPoolSize;
        public Class<? extends TopologyRackResolver> resolverCls;
    }

    public static class HBaseConfig implements Serializable {
        public String zkQuorum;
        public String zkRoot;
        public String zkClientPort;
        public String zkRetryTimes;
        public String hbaseMasterPrincipal;
        public String eaglePrincipal;
        public String eagleKeytab;
    }

    public static class MRConfig implements Serializable {
        public String[] rmUrls;
        public String historyServerUrl;
    }

    public static class HdfsConfig implements Serializable {
        public String[] namenodeUrls;
    }

    public static class SystemConfig implements Serializable {
        public String systemInstanceKafkaTopic;
        public String systemInstanceZkQuorum;
        public String systemInstanceKafkaSchemeCls;
        public int systemInstanceSendBatchSize;
    }

    private String getOptionalConfig(String key, String defaultValue) {
        if (this.config.hasPath(key)) {
            return config.getString(key);
        } else {
            return defaultValue;
        }
    }
}
