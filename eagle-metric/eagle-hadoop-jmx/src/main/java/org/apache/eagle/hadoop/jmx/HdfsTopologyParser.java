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

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.eagle.app.utils.connection.ServiceNotResponseException;
import org.apache.eagle.hadoop.jmx.model.HdfsServiceTopologyAPIEntity;
import org.apache.eagle.hadoop.jmx.model.TopologyResult;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONTokener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.eagle.hadoop.jmx.HadoopJmxConstant.*;
import static org.apache.eagle.hadoop.jmx.HadoopJmxUtil.generateMetric;

public class HdfsTopologyParser implements TopologyParser {
    private static final Logger LOG = LoggerFactory.getLogger(HdfsTopologyParser.class);

    private static final String COLON = ":";
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
    private static final String DATA_NODE_VERSION = "version";
    private static final String NAME_NODE_VERSION = "Version";

    private static final String DATA_NODE_DECOMMISSIONED = "Decommissioned";
    private static final String DATA_NODE_DECOMMISSIONED_STATE = "decommissioned";

    private static final String STATUS_PATTERN = "([\\d\\.]+):\\d+\\s+\\([\\D]+(\\d+)\\)";
    private static final String QJM_PATTERN = "([\\d\\.]+):\\d+";
    private String site;

    public HdfsTopologyParser(String site) {
        this.site = site;
    }

    @Override
    public TopologyResult parse(Map<String, JMXBean> data) throws IOException {
        final long updateTime = System.currentTimeMillis();
        final JMXBean bean = data.get(HadoopJmxConstant.FSNAMESYSTEM_BEAN);
        TopologyResult topologyResult = new TopologyResult();

        if (bean == null || bean.getPropertyMap() == null) {
            throw new IOException("Invalid JMX format, FSNamesystem bean is null!");
        }
        final String hostname = (String) bean.getPropertyMap().get(FS_HOSTNAME_TAG);
        HdfsServiceTopologyAPIEntity namenode = createHdfsServiceEntity(NAME_NODE_ROLE, hostname, updateTime);
        try {
            final String state = (String) bean.getPropertyMap().get(FS_HASTATE_TAG);
            final Double configuredCapacityGB = (Double) bean.getPropertyMap().get(CAPACITY_TOTAL_GB);
            final Double capacityUsedGB = (Double) bean.getPropertyMap().get(CAPACITY_USED_GB);
            final Integer blocksTotal = (Integer) bean.getPropertyMap().get(BLOCKS_TOTAL);

            namenode.setStatus(state);
            namenode.setConfiguredCapacityTB(Double.toString(configuredCapacityGB / FileUtils.ONE_KB));
            namenode.setUsedCapacityTB(Double.toString(capacityUsedGB / FileUtils.ONE_KB));
            namenode.setNumBlocks(Integer.toString(blocksTotal));

            final JMXBean nnBean = data.get(NAMENODEINFO_BEAN);
            if (nnBean == null || bean.getPropertyMap() == null) {
                throw new ServiceNotResponseException("Invalid JMX format, NameNodeInfo bean is null!");
            }
            String version = (String) bean.getPropertyMap().get("Version");
            namenode.setVersion(version);
            if (namenode.getStatus().equalsIgnoreCase(ACTIVE_STATE)) {
                createSlaveNodeEntities(updateTime, nnBean, topologyResult);
            }
        } catch (RuntimeException ex) {
            LOG.error(ex.getMessage(), ex);
        }
        topologyResult.getEntities().add(namenode);
        return topologyResult;
    }


    private void createSlaveNodeEntities(long updateTime, JMXBean nnBean, TopologyResult result) throws ServiceNotResponseException {
        createAllDataNodeEntities(nnBean, updateTime, result);
        createAllJournalNodeEntities(nnBean, updateTime, result);
    }

    private void createAllJournalNodeEntities(JMXBean bean, long updateTime, TopologyResult result) {
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
        String manager = jsonMap.getString("manager");
        Pattern qjm = Pattern.compile(QJM_PATTERN);
        Matcher jpmMatcher = qjm.matcher(manager);
        while (jpmMatcher.find()) {
            String ip = jpmMatcher.group(1);
            String hostname = resolveHostByIp(ip);
            HdfsServiceTopologyAPIEntity entity = createHdfsServiceEntity(JOURNAL_NODE_ROLE, hostname, updateTime);
            entity.setStatus(DATA_NODE_DEAD_STATUS);
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
                journalNodesMap.get(ip).setStatus(DATA_NODE_LIVE_STATUS);
            }
        }
        result.getEntities().addAll(journalNodesMap.values());

        double value = numLiveJournalNodes * 1d / journalNodesMap.size();
        result.getMetrics().add(generateMetric(JOURNAL_NODE_ROLE, value, site, updateTime));
    }

    private void createAllDataNodeEntities(JMXBean bean, long updateTime, TopologyResult result) throws JSONException {
        int numLiveNodes = 0;
        int numLiveDecommNodes = 0;
        int numDeadNodes = 0;
        int numDeadDecommNodes = 0;

        String deadNodesStrings = (String) bean.getPropertyMap().get(DEAD_NODES);
        JSONTokener tokener = new JSONTokener(deadNodesStrings);
        JSONObject jsonNodesObject = new JSONObject(tokener);
        final JSONArray deadNodes = jsonNodesObject.names();
        for (int i = 0; deadNodes != null && i < deadNodes.length(); ++i) {
            final String hostname = deadNodes.getString(i);
            final JSONObject deadNode = jsonNodesObject.getJSONObject(hostname);
            HdfsServiceTopologyAPIEntity entity = createHdfsServiceEntity(DATA_NODE_ROLE, getValidHostName(hostname), updateTime);
            if (deadNode.getBoolean(DATA_NODE_DECOMMISSIONED_STATE)) {
                ++numDeadDecommNodes;
                entity.setStatus(DATA_NODE_DEAD_DECOMMISSIONED_STATUS);
            } else {
                entity.setStatus(DATA_NODE_DEAD_STATUS);
            }
            ++numDeadNodes;
            result.getEntities().add(entity);
        }
        LOG.info("Dead nodes " + numDeadNodes + ", dead but decommissioned nodes: " + numDeadDecommNodes);

        String liveNodesStrings = (String) bean.getPropertyMap().get(LIVE_NODES);
        tokener = new JSONTokener(liveNodesStrings);
        jsonNodesObject = new JSONObject(tokener);
        final JSONArray liveNodes = jsonNodesObject.names();
        for (int i = 0; liveNodes != null && i < liveNodes.length(); ++i) {
            final String hostname = liveNodes.getString(i);
            final JSONObject liveNode = jsonNodesObject.getJSONObject(hostname);

            HdfsServiceTopologyAPIEntity entity = createHdfsServiceEntity(DATA_NODE_ROLE, getValidHostName(hostname), updateTime);
            final Number configuredCapacity = (Number) liveNode.get(DATA_NODE_CAPACITY);
            entity.setConfiguredCapacityTB(Double.toString(configuredCapacity.doubleValue() / FileUtils.ONE_TB));
            final Number capacityUsed = (Number) liveNode.get(DATA_NODE_USED_SPACE);
            entity.setUsedCapacityTB(Double.toString(capacityUsed.doubleValue() / FileUtils.ONE_TB));
            final Number blocksTotal = (Number) liveNode.get(DATA_NODE_NUM_BLOCKS);
            entity.setNumBlocks(Double.toString(blocksTotal.doubleValue()));
            if (liveNode.has(DATA_NODE_FAILED_VOLUMN)) {
                final Number volFails = (Number) liveNode.get(DATA_NODE_FAILED_VOLUMN);
                entity.setNumFailedVolumes(Double.toString(volFails.doubleValue()));
            }
            final String adminState = liveNode.getString(DATA_NODE_ADMIN_STATE);
            if (DATA_NODE_DECOMMISSIONED.equalsIgnoreCase(adminState)) {
                ++numLiveDecommNodes;
                entity.setStatus(DATA_NODE_LIVE_DECOMMISSIONED_STATUS);
            } else {
                entity.setStatus(DATA_NODE_LIVE_STATUS);
            }
            entity.setVersion(String.valueOf(liveNode.get(DATA_NODE_VERSION)));
            numLiveNodes++;
            result.getEntities().add(entity);
        }
        LOG.info("Live nodes " + numLiveNodes + ", live but decommissioned nodes: " + numLiveDecommNodes);

        double value = numLiveNodes * 1.0d / (numLiveNodes + numDeadNodes);
        result.getMetrics().add(generateMetric(DATA_NODE_ROLE, value, site, updateTime));
    }

    private HdfsServiceTopologyAPIEntity createHdfsServiceEntity(String roleType, String hostname, long updateTime) {
        HdfsServiceTopologyAPIEntity entity = new HdfsServiceTopologyAPIEntity();
        entity.setTimestamp(updateTime);
        entity.setLastUpdateTime(updateTime);
        Map<String, String> tags = new HashMap<String, String>();
        entity.setTags(tags);
        tags.put(SITE_TAG, site);
        tags.put(ROLE_TAG, roleType);
        tags.put(HOSTNAME_TAG, hostname);
        tags.put(RACK_TAG, "null");
        return entity;
    }

    private String getValidHostName(String key) {
        if (StringUtils.isBlank(key)) {
            throw new IllegalArgumentException("key can not be empty");
        }
        return key.indexOf(COLON) > 0 ? key.substring(0, key.indexOf(COLON)) : key;
    }

    private String resolveHostByIp(String ip) {
        InetAddress addr = null;
        try {
            addr = InetAddress.getByName(ip);
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
        return addr.getHostName();
    }


}

