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

package org.apache.eagle.topology.extractor.hdfs;

import org.apache.eagle.app.utils.PathResolverHelper;
import org.apache.eagle.topology.TopologyCheckAppConfig;
import org.apache.eagle.topology.TopologyConstants;
import org.apache.eagle.topology.extractor.TopologyEntityParserResult;
import org.apache.eagle.topology.entity.HdfsServiceTopologyAPIEntity;
import org.apache.eagle.topology.extractor.TopologyEntityParser;
import org.apache.eagle.topology.resolver.TopologyRackResolver;
import org.apache.eagle.topology.utils.*;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONTokener;
import org.slf4j.Logger;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.eagle.topology.TopologyConstants.*;
import static org.apache.eagle.topology.TopologyConstants.RACK_TAG;

public class HdfsTopologyEntityParser implements TopologyEntityParser {

    private static final Logger LOG = org.slf4j.LoggerFactory.getLogger(HdfsTopologyEntityParser.class);
    private String [] namenodeUrls;
    private String site;
    private TopologyRackResolver rackResolver;

    private static final String JMX_URL = "/jmx?anonymous=true";
    private static final String JMX_FS_NAME_SYSTEM_BEAN_NAME = "Hadoop:service=NameNode,name=FSNamesystem";
    private static final String JMX_NAMENODE_INFO = "Hadoop:service=NameNode,name=NameNodeInfo";

    private static final String HA_STATE = "tag.HAState";
    private static final String HA_NAME = "tag.Hostname";
    private static final String CAPACITY_TOTAL_GB = "CapacityTotalGB";
    private static final String CAPACITY_USED_GB = "CapacityUsedGB";
    private static final String BLOCKS_TOTAL = "BlocksTotal";
    private static final String LIVE_NODES = "LiveNodes";
    private static final String DEAD_NODES = "DeadNodes";

    private static final String JN_STATUS = "NameJournalStatus";
    private static final String JN_TRANSACTION_INFO = "JournalTransactionInfo";
    private static final String LAST_TX_ID = "LastAppliedOrWrittenTxId";

    private static final String DATA_NODE_NUM_BLOCKS = "numBlocks";
    private static final String DATA_NODE_USED_SPACE = "usedSpace";
    private static final String DATA_NODE_CAPACITY = "capacity";
    private static final String DATA_NODE_ADMIN_STATE = "adminState";
    private static final String DATA_NODE_FAILED_VOLUMN = "volfails";

    private static final String DATA_NODE_DECOMMISSIONED = "Decommissioned";
    private static final String DATA_NODE_DECOMMISSIONED_STATE = "decommissioned";

    private static final String STATUS_PATTERN = "([\\d\\.]+):\\d+\\s+\\([\\D]+(\\d+)\\)";
    private static final String QJM_PATTERN = "([\\d\\.]+):\\d+";
    
    private static final double TB = 1024 * 1024 * 1024 * 1024;

    public HdfsTopologyEntityParser(String site, TopologyCheckAppConfig.HdfsConfig hdfsConfig, TopologyRackResolver rackResolver) {
        this.namenodeUrls = hdfsConfig.namenodeUrls;
        this.site = site;
        this.rackResolver = rackResolver;
    }

    @Override
    public TopologyEntityParserResult parse(long timestamp) throws IOException {
        final TopologyEntityParserResult result = new TopologyEntityParserResult();
        result.setVersion(TopologyConstants.HadoopVersion.V2);
        int numNamenode = 0;
        for (String url : namenodeUrls) {
            try {
                final HdfsServiceTopologyAPIEntity namenodeEntity = createNamenodeEntity(url, timestamp);
                result.getMasterNodes().add(namenodeEntity);
                numNamenode++;
                if (namenodeEntity.getStatus().equalsIgnoreCase(NAME_NODE_ACTIVE_STATUS)) {
                    createSlaveNodeEntities(url, timestamp, result);
                }
            } catch (RuntimeException ex) {
                ex.printStackTrace();
            } catch (IOException e) {
                LOG.warn("Catch an IOException with url: {}", url);
            }
        }
        double value = numNamenode * 1d / namenodeUrls.length;
        result.getMetrics().add(EntityBuilderHelper.generateMetric(TopologyConstants.NAME_NODE_ROLE, value, site, timestamp));
        return result;
    }

    private HdfsServiceTopologyAPIEntity createNamenodeEntity(String url, long updateTime) throws JSONException, IOException {
        final String urlString = buildFSNamesystemURL(url);
        final Map<String, JMXBean> jmxBeanMap = JMXQueryHelper.query(urlString);
        final JMXBean bean = jmxBeanMap.get(JMX_FS_NAME_SYSTEM_BEAN_NAME);
        if (bean == null || bean.getPropertyMap() == null) {
            throw new ServiceNotResponseException("Invalid JMX format, FSNamesystem bean is null!");
        }
        final String hostname = (String)bean.getPropertyMap().get(HA_NAME);
        HdfsServiceTopologyAPIEntity result = createHdfsServiceEntity(TopologyConstants.NAME_NODE_ROLE, hostname, updateTime);
        final String state = (String)bean.getPropertyMap().get(HA_STATE);
        result.setStatus(state);
        final Double configuredCapacityGB = (Double) bean.getPropertyMap().get(CAPACITY_TOTAL_GB);
        result.setConfiguredCapacityTB(Double.toString(configuredCapacityGB / 1024));
        final Double capacityUsedGB = (Double) bean.getPropertyMap().get(CAPACITY_USED_GB);
        result.setUsedCapacityTB(Double.toString(capacityUsedGB / 1024));
        final Integer blocksTotal = (Integer) bean.getPropertyMap().get(BLOCKS_TOTAL);
        result.setNumBlocks(Integer.toString(blocksTotal));
        return result;
    }

    private void createSlaveNodeEntities(String url, long updateTime, TopologyEntityParserResult result) throws IOException {
        final String urlString = buildNamenodeInfo(url);
        final Map<String, JMXBean> jmxBeanMap = JMXQueryHelper.query(urlString);
        final JMXBean bean = jmxBeanMap.get(JMX_NAMENODE_INFO);
        if (bean == null || bean.getPropertyMap() == null) {
            throw new ServiceNotResponseException("Invalid JMX format, NameNodeInfo bean is null!");
        }
        createAllDataNodeEntities(bean, updateTime, result);
        createAllJournalNodeEntities(bean, updateTime, result);
    }

    private void createAllJournalNodeEntities(JMXBean bean, long updateTime, TopologyEntityParserResult result) throws UnknownHostException {
        if (bean.getPropertyMap().get(JN_TRANSACTION_INFO) == null || bean.getPropertyMap().get(JN_STATUS) == null) {
            return;
        }
        String jnInfoString = (String) bean.getPropertyMap().get(JN_TRANSACTION_INFO);
        JSONObject jsonObject = new JSONObject(jnInfoString);
        long lastTxId = Long.parseLong(jsonObject.getString(LAST_TX_ID));

        String journalnodeString = (String) bean.getPropertyMap().get(JN_STATUS);
        JSONArray jsonArray = new JSONArray(journalnodeString);
        JSONObject jsonMap = (JSONObject) jsonArray.get(0);

        Map<String, HdfsServiceTopologyAPIEntity> journalNodesMap = new HashMap<>();
        String QJM = jsonMap.getString("manager");
        Pattern qjm = Pattern.compile(QJM_PATTERN);
        Matcher jpmMatcher = qjm.matcher(QJM);
        while (jpmMatcher.find()) {
            String ip = jpmMatcher.group(1);
            String hostname = EntityBuilderHelper.resolveHostByIp(ip);
            HdfsServiceTopologyAPIEntity entity = createHdfsServiceEntity(TopologyConstants.JOURNAL_NODE_ROLE, hostname, updateTime);
            entity.setStatus(TopologyConstants.DATA_NODE_DEAD_STATUS);
            journalNodesMap.put(ip, entity);
        }
        if (journalNodesMap.isEmpty()) {
            LOG.warn("Fail to find journal node info in JMX");
            return;
        }

        String stream = jsonMap.getString("stream");
        Pattern status = Pattern.compile(STATUS_PATTERN);
        Matcher statusMatcher = status.matcher(stream);
        long numLiveJournalNodes = 0;
        while (statusMatcher.find()) {
            numLiveJournalNodes++;
            String ip = statusMatcher.group(1);
            if (journalNodesMap.containsKey(ip)) {
                long txid = Long.parseLong(statusMatcher.group(2));
                journalNodesMap.get(ip).setWrittenTxidDiff(lastTxId - txid);
                journalNodesMap.get(ip).setStatus(TopologyConstants.DATA_NODE_LIVE_STATUS);
            }
        }
        result.getMasterNodes().addAll(journalNodesMap.values());

        double value = numLiveJournalNodes * 1d / journalNodesMap.size();
        result.getMetrics().add(EntityBuilderHelper.generateMetric(TopologyConstants.JOURNAL_NODE_ROLE, value, site, updateTime));
    }

    private void createAllDataNodeEntities(JMXBean bean, long updateTime, TopologyEntityParserResult result) throws JSONException, IOException {
        int numLiveNodes = 0;
        int numLiveDecommNodes = 0;
        int numDeadNodes = 0;
        int numDeadDecommNodes = 0;

        String deadNodesStrings = (String) bean.getPropertyMap().get(DEAD_NODES);
        JSONTokener tokener  = new JSONTokener(deadNodesStrings);
        JSONObject jsonNodesObject = new JSONObject(tokener);
        final JSONArray deadNodes = jsonNodesObject.names();
        for (int i = 0; deadNodes != null && i < deadNodes.length(); ++i) {
            final String hostname = deadNodes.getString(i);
            final JSONObject deadNode = jsonNodesObject.getJSONObject(hostname);
            HdfsServiceTopologyAPIEntity entity = createHdfsServiceEntity(TopologyConstants.DATA_NODE_ROLE, EntityBuilderHelper.getValidHostName(hostname), updateTime);
            if (deadNode.getBoolean(DATA_NODE_DECOMMISSIONED_STATE)) {
                ++numDeadDecommNodes;
                entity.setStatus(TopologyConstants.DATA_NODE_DEAD_DECOMMISSIONED_STATUS);
            } else {
                entity.setStatus(TopologyConstants.DATA_NODE_DEAD_STATUS);
            }
            ++numDeadNodes;
            result.getSlaveNodes().add(entity);
        }
        LOG.info("Dead nodes " + numDeadNodes + ", dead but decommissioned nodes: " + numDeadDecommNodes);

        String liveNodesStrings = (String) bean.getPropertyMap().get(LIVE_NODES);
        tokener = new JSONTokener(liveNodesStrings);
        jsonNodesObject = new JSONObject(tokener);
        final JSONArray liveNodes = jsonNodesObject.names();
        for (int i = 0; liveNodes != null && i < liveNodes.length(); ++i) {
            final String hostname = liveNodes.getString(i);
            final JSONObject liveNode = jsonNodesObject.getJSONObject(hostname);

            HdfsServiceTopologyAPIEntity entity = createHdfsServiceEntity(TopologyConstants.DATA_NODE_ROLE, EntityBuilderHelper.getValidHostName(hostname), updateTime);
            final Number configuredCapacity = (Number) liveNode.get(DATA_NODE_CAPACITY);
            entity.setConfiguredCapacityTB(Double.toString(configuredCapacity.doubleValue() / TB));
            final Number capacityUsed = (Number) liveNode.get(DATA_NODE_USED_SPACE);
            entity.setUsedCapacityTB(Double.toString(capacityUsed.doubleValue() / TB));
            final Number blocksTotal = (Number) liveNode.get(DATA_NODE_NUM_BLOCKS);
            entity.setNumBlocks(Double.toString(blocksTotal.doubleValue()));
            if (liveNode.has(DATA_NODE_FAILED_VOLUMN)) {
                final Number volFails = (Number) liveNode.get(DATA_NODE_FAILED_VOLUMN);
                entity.setNumFailedVolumes(Double.toString(volFails.doubleValue()));
            }
            final String adminState = liveNode.getString(DATA_NODE_ADMIN_STATE);
            if (DATA_NODE_DECOMMISSIONED.equalsIgnoreCase(adminState)) {
                ++numLiveDecommNodes;
                entity.setStatus(TopologyConstants.DATA_NODE_LIVE_DECOMMISSIONED_STATUS);
            } else {
                entity.setStatus(TopologyConstants.DATA_NODE_LIVE_STATUS);
            }
            numLiveNodes++;
            result.getSlaveNodes().add(entity);
        }
        LOG.info("Live nodes " + numLiveNodes + ", live but decommissioned nodes: " + numLiveDecommNodes);

        double value = numLiveNodes * 1.0d / result.getSlaveNodes().size();
        result.getMetrics().add(EntityBuilderHelper.generateMetric(TopologyConstants.DATA_NODE_ROLE, value, site, updateTime));
    }

    private HdfsServiceTopologyAPIEntity createHdfsServiceEntity(String roleType, String hostname, long updateTime) {
        HdfsServiceTopologyAPIEntity entity = new HdfsServiceTopologyAPIEntity();
        entity.setTimestamp(updateTime);
        Map<String, String> tags = new HashMap<String, String>();
        entity.setTags(tags);
        tags.put(SITE_TAG, site);
        tags.put(ROLE_TAG, roleType);
        tags.put(HOSTNAME_TAG, hostname);
        String rack = rackResolver.resolve(hostname);
        tags.put(RACK_TAG, rack);
        return entity;
    }

    private String buildFSNamesystemURL(String url) {
        return PathResolverHelper.buildUrlPath(url, JMX_URL + "&qry=" + JMX_FS_NAME_SYSTEM_BEAN_NAME);
    }

    private String buildNamenodeInfo(String url) {
        return PathResolverHelper.buildUrlPath(url, JMX_URL + "&qry=" + JMX_NAMENODE_INFO);
    }

    @Override
    public TopologyConstants.TopologyType getTopologyType() {
        return TopologyConstants.TopologyType.HDFS;
    }

    @Override
    public TopologyConstants.HadoopVersion getHadoopVersion() {
        return TopologyConstants.HadoopVersion.V2;
    }
}
