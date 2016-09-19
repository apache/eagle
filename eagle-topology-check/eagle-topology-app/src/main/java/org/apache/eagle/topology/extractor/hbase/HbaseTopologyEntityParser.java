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
import org.apache.eagle.log.entity.GenericMetricEntity;
import org.apache.eagle.topology.TopologyCheckAppConfig;
import org.apache.eagle.topology.TopologyConstants;
import org.apache.eagle.topology.TopologyEntityParserResult;
import org.apache.eagle.topology.entity.HBaseServiceTopologyAPIEntity;
import org.apache.eagle.topology.entity.MRServiceTopologyAPIEntity;
import org.apache.eagle.topology.extractor.TopologyEntityParser;
import org.apache.eagle.topology.utils.EntityBuilderHelper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.ServerLoad;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.HBaseAdmin;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import static org.apache.eagle.topology.TopologyConstants.*;

public class HbaseTopologyEntityParser implements TopologyEntityParser {

    private Configuration hBaseConfiguration;
    private String site;

    public  HbaseTopologyEntityParser(String site, TopologyCheckAppConfig.HBaseConfig hBaseConfig) {
        this.site = site;
        this.hBaseConfiguration = HBaseConfiguration.create();
        this.hBaseConfiguration.set("hbase.zookeeper.quorum", hBaseConfig.zkQuorum);
        this.hBaseConfiguration.set("hbase.zookeeper.property.clientPort", "2181");
        this.hBaseConfiguration.set("zookeeper.znode.parent", hBaseConfig.zkRoot);
        this.hBaseConfiguration.set(HadoopSecurityUtil.EAGLE_USER_NAME_KEY, hBaseConfig.principal);
        this.hBaseConfiguration.set(HadoopSecurityUtil.EAGLE_KEYTAB_FILE_KEY, hBaseConfig.keytab);
    }

    private HBaseAdmin getHBaseAdmin() throws IOException {
        HadoopSecurityUtil.login(hBaseConfiguration);
        return new HBaseAdmin(this.hBaseConfiguration);
    }

    @Override
    public TopologyEntityParserResult parse(long timestamp) {
        long deadServers = 0;
        long liveServers = 0;
        HBaseAdmin admin = null;
        try {
            admin = getHBaseAdmin();
            ClusterStatus status = admin.getClusterStatus();
            deadServers = status.getDeadServers();
            liveServers = status.getServersSize();
            TopologyEntityParserResult result = new TopologyEntityParserResult();
            result.setVersion(HadoopVersion.V2);
            result.getNodes().put(TopologyConstants.HMASTER_ROLE, new ArrayList<>());
            result.getNodes().put(TopologyConstants.REGIONSERVER_ROLE, new ArrayList<>());
            for (ServerName server : status.getServers()) {
                HBaseServiceTopologyAPIEntity entity = createEntity(TopologyConstants.REGIONSERVER_ROLE, server.getHostname(), timestamp);
                ServerLoad load = status.getLoad(server);
                parseServerLoad(entity, load);
                entity.setStatus(TopologyConstants.REGIONSERVER_LIVE_STATUS);
                result.getNodes().get(TopologyConstants.REGIONSERVER_ROLE).add(entity);
            }
            ServerName master = status.getMaster();
            if (master != null) {
                HBaseServiceTopologyAPIEntity masterEntity = createEntity(TopologyConstants.HMASTER_ROLE, master.getHostname(), timestamp);
                ServerLoad load = status.getLoad(master);
                parseServerLoad(masterEntity, load);
                masterEntity.setStatus(TopologyConstants.HMASTER_ACTIVE_STATUS);
                result.getNodes().get(TopologyConstants.HMASTER_ROLE).add(masterEntity);
            }
            for (ServerName server : status.getBackupMasters()) {
                HBaseServiceTopologyAPIEntity entity = createEntity(TopologyConstants.HMASTER_ROLE, server.getHostname(), timestamp);
                ServerLoad load = status.getLoad(server);
                parseServerLoad(entity, load);
                entity.setStatus(TopologyConstants.HMASTER_STANDBY_STATUS);
                result.getNodes().get(TopologyConstants.REGIONSERVER_ROLE).add(entity);
            }
            double liveRatio = liveServers * 1d / (liveServers + deadServers);
            result.getMetrics().add(EntityBuilderHelper.generateMetric(TopologyConstants.REGIONSERVER_ROLE, liveRatio, site, timestamp));
            return result;
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (admin != null) {
                try {
                    admin.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return null;
    }

    private void parseServerLoad(HBaseServiceTopologyAPIEntity entity, ServerLoad load) {
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
        String rack = EntityBuilderHelper.resolveRackByHost(hostname);
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
