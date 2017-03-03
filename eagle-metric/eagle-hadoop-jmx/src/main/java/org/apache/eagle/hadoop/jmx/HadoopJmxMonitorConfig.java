/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.eagle.hadoop.jmx;

import com.typesafe.config.Config;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

public class HadoopJmxMonitorConfig {
    public List<String> filterGroup;
    public List<String> filterMetrics;
    public Config config;
    public String site;
    public Topology topology;
    public DataSourceConfig dataSourceConfig;

    public static final String SPOUT_NAME = "JmxReader";
    public static final String TOPOLOGY_PARSER_NAME = "topologyParser";
    public static final String METRIC_GENERATOR_NAME = "metricGenerator";

    public static final String HASTATE_METRIC_STREAM = "haStateMetricStream";
    public static final String SPOUT_TO_TOPOLGY_STREAM = "reader-to-topologyParser";
    public static final String SPOUT_TO_METRIC_STREAM= "reader-to-metricParser";
    public static final String TOPOLOGY_CHECK_STREAM = "topologyCheckStream";
    public static final String JMX_METRCI_STREAM = "jmxMetricStream";
    public static final String JMX_RESOURCE_METRIC_STRAEM = "jmxResourceMetricStream";

    private HadoopJmxMonitorConfig(Config config) {
        this.config = config;
        init(config);
    }

    private void init(Config config) {
        this.site = config.getString("siteId");
        topology.fetchDataIntervalInSecs = config.getLong("topology.fetchDataIntervalInSecs");
        topology.checkSinkBoltNum = config.getInt("topology.check.numOfKafkaSinkBolt");
        topology.metricSinkBoltNum = config.getInt("topology.metric.numOfKafkaSinkBolt");
        topology.rpcSinkBoltNum = config.getInt("topology.rpc.numOfKafkaSinkBolt");
        topology.persistentBoltNum = config.getInt("topology.numOfPersistentBolt");

        dataSourceConfig.hmasterUrls = config.getString("dataSourceConfig.hbasemasterUrl").split(",\\s*");
        dataSourceConfig.namenodeUrls = config.getString("dataSourceConfig.namenodeUrl").split(",\\s*");
        dataSourceConfig.rmUrls = config.getString("dataSourceConfig.resourcemanagerUrl").split(",\\s*");
        dataSourceConfig.resolverClass = config.getString("dataSourceConfig.rackResolverCls");

        filterGroup = Arrays.asList(config.getString("filter.beanGroup").split(",\\s*"));
        filterMetrics = Arrays.asList(config.getString("filter.metricName").split(",\\s*"));
    }

    public static class Topology {
        public long fetchDataIntervalInSecs;
        public int checkSinkBoltNum;
        public int rpcSinkBoltNum;
        public int metricSinkBoltNum;
        public int persistentBoltNum;
    }

    public static class DataSourceConfig implements Serializable {
        public String[] namenodeUrls;
        public String[] hmasterUrls;
        public String[] rmUrls;
        public String resolverClass;
    }


}
