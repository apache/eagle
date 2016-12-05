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

package org.apache.eagle.topology.extractor.hbase;

import org.apache.eagle.app.utils.HadoopSecurityUtil;
import org.apache.eagle.topology.TopologyCheckAppConfig;
import org.apache.eagle.topology.TopologyConstants;
import org.apache.eagle.topology.extractor.TopologyEntityParserResult;
import org.apache.eagle.topology.entity.HBaseServiceTopologyAPIEntity;
import org.apache.eagle.topology.extractor.TopologyEntityParser;
import org.apache.eagle.topology.resolver.TopologyRackResolver;
import org.apache.eagle.topology.utils.EntityBuilderHelper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.ServerLoad;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.slf4j.Logger;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.apache.eagle.topology.TopologyConstants.*;

public class HbaseTopologyEntityParser implements TopologyEntityParser {

    private static final Logger LOG = org.slf4j.LoggerFactory.getLogger(HbaseTopologyEntityParser.class);
    private Configuration hBaseConfiguration;
    private String site;
    private Boolean kerberosEnable = false;
    private TopologyRackResolver rackResolver;

    public HbaseTopologyEntityParser(String site, TopologyCheckAppConfig.HBaseConfig hBaseConfig, TopologyRackResolver resolver) {
        this.site = site;
        this.rackResolver = resolver;
        this.hBaseConfiguration = HBaseConfiguration.create();
        this.hBaseConfiguration.set("hbase.zookeeper.quorum", hBaseConfig.zkQuorum);
        this.hBaseConfiguration.set("hbase.zookeeper.property.clientPort", hBaseConfig.zkClientPort);
        this.hBaseConfiguration.set("zookeeper.znode.parent", hBaseConfig.zkRoot);
        this.hBaseConfiguration.set("hbase.client.retries.number", hBaseConfig.zkRetryTimes);
        // kerberos authentication
        if (hBaseConfig.eaglePrincipal != null && hBaseConfig.eagleKeytab != null
            && !hBaseConfig.eaglePrincipal.isEmpty() && !hBaseConfig.eagleKeytab.isEmpty()) {
            this.hBaseConfiguration.set(HadoopSecurityUtil.EAGLE_PRINCIPAL_KEY, hBaseConfig.eaglePrincipal);
            this.hBaseConfiguration.set(HadoopSecurityUtil.EAGLE_KEYTAB_FILE_KEY, hBaseConfig.eagleKeytab);
            this.kerberosEnable = true;
            this.hBaseConfiguration.set("hbase.security.authentication", "kerberos");
            this.hBaseConfiguration.set("hbase.master.kerberos.principal", hBaseConfig.hbaseMasterPrincipal);
        }
    }

    private HBaseAdmin getHBaseAdmin() throws IOException {
        if (this.kerberosEnable) {
            HadoopSecurityUtil.login(hBaseConfiguration);
        }
        return new HBaseAdmin(this.hBaseConfiguration);
    }


    @Override
    public TopologyEntityParserResult parse(long timestamp) {
        final TopologyEntityParserResult result = new TopologyEntityParserResult();
        int activeRatio = 0;
        try {
            doParse(timestamp, result);
            activeRatio++;
        } catch (Exception ex) {
            LOG.error(ex.getMessage(), ex);
        }
        result.getMetrics().add(EntityBuilderHelper.generateMetric(TopologyConstants.HMASTER_ROLE, activeRatio, site, timestamp));
        return result;
    }

    private void doParse(long timestamp, TopologyEntityParserResult result) throws IOException {
        long deadServers = 0;
        long liveServers = 0;
        HBaseAdmin admin = null;
        try {
            admin = getHBaseAdmin();
            ClusterStatus status = admin.getClusterStatus();
            deadServers = status.getDeadServers();
            liveServers = status.getServersSize();

            for (ServerName liveServer : status.getServers()) {
                ServerLoad load = status.getLoad(liveServer);
                result.getSlaveNodes().add(parseServer(liveServer, load, TopologyConstants.REGIONSERVER_ROLE, TopologyConstants.REGIONSERVER_LIVE_STATUS, timestamp));
            }
            for (ServerName deadServer : status.getDeadServerNames()) {
                ServerLoad load = status.getLoad(deadServer);
                result.getSlaveNodes().add(parseServer(deadServer, load, TopologyConstants.REGIONSERVER_ROLE, TopologyConstants.REGIONSERVER_DEAD_STATUS, timestamp));
            }
            ServerName master = status.getMaster();
            if (master != null) {
                ServerLoad load = status.getLoad(master);
                result.getMasterNodes().add(parseServer(master, load, TopologyConstants.HMASTER_ROLE, TopologyConstants.HMASTER_ACTIVE_STATUS, timestamp));
            }
            for (ServerName backupMaster : status.getBackupMasters()) {
                ServerLoad load = status.getLoad(backupMaster);
                result.getMasterNodes().add(parseServer(backupMaster, load, TopologyConstants.HMASTER_ROLE, TopologyConstants.HMASTER_STANDBY_STATUS, timestamp));
            }
            double liveRatio = liveServers * 1d / (liveServers + deadServers);
            result.getMetrics().add(EntityBuilderHelper.generateMetric(TopologyConstants.REGIONSERVER_ROLE, liveRatio, site, timestamp));
            LOG.info("live servers: {}, dead servers: {}", liveServers, deadServers);
        } finally {
            if (admin != null) {
                try {
                    admin.close();
                } catch (IOException e) {
                    LOG.error(e.getMessage(), e);
                }
            }
        }
    }

    private HBaseServiceTopologyAPIEntity parseServer(ServerName serverName, ServerLoad serverLoad, String role, String status, long timestamp) {
        if (serverName == null) {
            return null;
        }
        HBaseServiceTopologyAPIEntity entity = createEntity(role, serverName.getHostname(), timestamp);
        parseServerLoad(entity, serverLoad);
        entity.setStatus(status);
        return entity;
    }

    private void parseServerLoad(HBaseServiceTopologyAPIEntity entity, ServerLoad load) {
        if (load == null) {
            return;
        }
        entity.setMaxHeapMB(load.getMaxHeapMB());
        entity.setUsedHeapMB(load.getUsedHeapMB());
        entity.setNumRegions(load.getNumberOfRegions());
        entity.setNumRequests(load.getNumberOfRequests());
    }

    private HBaseServiceTopologyAPIEntity createEntity(String roleType, String hostname, long timestamp) {
        HBaseServiceTopologyAPIEntity entity = new HBaseServiceTopologyAPIEntity();
        Map<String, String> tags = new HashMap<String, String>();
        entity.setTags(tags);
        tags.put(SITE_TAG, site);
        tags.put(ROLE_TAG, roleType);
        tags.put(HOSTNAME_TAG, hostname);
        String rack = rackResolver.resolve(hostname);
        tags.put(RACK_TAG, rack);
        entity.setLastUpdateTime(timestamp);
        entity.setTimestamp(timestamp);
        return entity;
    }

    @Override
    public TopologyConstants.TopologyType getTopologyType() {
        return TopologyType.HBASE;
    }

    @Override
    public TopologyConstants.HadoopVersion getHadoopVersion() {
        return HadoopVersion.V2;
    }
}
