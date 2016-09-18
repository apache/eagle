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

import org.apache.eagle.log.entity.GenericMetricEntity;
import org.apache.eagle.topology.TopologyCheckAppConfig;
import org.apache.eagle.topology.TopologyConstants;
import org.apache.eagle.topology.TopologyEntityParserResult;
import org.apache.eagle.topology.entity.HdfsServiceTopologyAPIEntity;
import org.apache.eagle.topology.entity.JournalNodeServiceAPIEntity;
import org.apache.eagle.topology.entity.TopologyBaseAPIEntity;
import org.apache.eagle.topology.extractor.TopologyEntityParser;
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
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.eagle.topology.TopologyConstants.JOURNAL_NODE_ROLE;
import static org.apache.eagle.topology.TopologyConstants.NAME_NODE_ACTIVE_STATUS;

public class HdfsTopologyEntityParser implements TopologyEntityParser {

    private static final Logger LOG = org.slf4j.LoggerFactory.getLogger(HdfsTopologyEntityParser.class);
    private String [] namenodeUrls;
    private String site;

    private static final String JMX_URL = "jmx?anonymous=true";
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
    private static final String DATA_NODE_DECOMMISSIONED = "Decommissioned";
    private static final String DATA_NODE_DECOMMISSIONED_STATE = "decommissioned";

    private static final String METRIC_NAME_FORMAT = "hadoop.%s.liveRatio";

    private static final String STATUS_PATTERN = "([\\d\\.]+):\\d+\\s+\\([\\D]+(\\d+)\\)";
    private static final String QJM_PATTERN = "([\\d\\.]+):\\d+";

    public HdfsTopologyEntityParser(String site, TopologyCheckAppConfig.HdfsConfig hdfsConfig) {
        this.namenodeUrls = hdfsConfig.namenodeUrls;
        this.site = site;
    }

    @Override
    public TopologyEntityParserResult parse() {
        final TopologyEntityParserResult result = new TopologyEntityParserResult();
        result.setVersion(TopologyConstants.HadoopVersion.V2);
        for (String url : namenodeUrls) {
            try {
                final HdfsServiceTopologyAPIEntity namenodeEntity = createNamenodeEntity(url);
                if (!result.getNodes().containsKey(TopologyConstants.NAME_NODE_ROLE)) {
                    result.getNodes().put(TopologyConstants.NAME_NODE_ROLE, new ArrayList());
                }
                result.getNodes().get(TopologyConstants.NAME_NODE_ROLE).add(namenodeEntity);
                if (namenodeEntity.getTags().get(HA_STATE).equalsIgnoreCase(NAME_NODE_ACTIVE_STATUS)) {
                    createSlaveNodeEntities(url, result);
                }
                return result;
            } catch (IOException ex) {
                ex.printStackTrace();
            }
        }
        double value = result.getNodes().get(TopologyConstants.NAME_NODE_ROLE).size() * 1d / namenodeUrls.length;
        result.getMetrics().add(generateMetric(TopologyConstants.DATA_NODE_ROLE, value));
        return result;
    }

    private HdfsServiceTopologyAPIEntity createNamenodeEntity(String url) throws JSONException, IOException {
        final HdfsServiceTopologyAPIEntity result = new HdfsServiceTopologyAPIEntity();
        result.setTags(new HashMap<>());
        final String urlString = buildFSNamesystemURL(url);
        final Map<String, JMXBean> jmxBeanMap = JMXQueryHelper.query(urlString);
        final JMXBean bean = jmxBeanMap.get(TopologyResourceURLBuilder.HDFS_JMX_FS_SYSTEM_BEAN);
        if (bean == null || bean.getPropertyMap() == null) {
            throw new ServiceNotResponseException("Invalid JMX format, FSNamesystem bean is null!");
        }
        final String state = (String)bean.getPropertyMap().get(HA_STATE);
        result.setStatus(state);
        final String hostname = (String)bean.getPropertyMap().get(HA_NAME);
        result.getTags().put(TopologyConstants.HOSTNAME_TAG, hostname);
        result.getTags().put(TopologyConstants.RACK_TAG, EntityBuilderHelper.resolveRackByHost(hostname));
        result.getTags().put(TopologyConstants.ROLE_TAG, TopologyConstants.NAME_NODE_ROLE);
        final Double configuredCapacityGB = (Double) bean.getPropertyMap().get(CAPACITY_TOTAL_GB);
        result.setConfiguredCapacityTB(Double.toString(configuredCapacityGB / 1024));
        final Double capacityUsedGB = (Double) bean.getPropertyMap().get(CAPACITY_USED_GB);
        result.setUsedCapacityTB(Double.toString(capacityUsedGB / 1024));
        final Integer blocksTotal = (Integer) bean.getPropertyMap().get(BLOCKS_TOTAL);
        result.setNumBlocks(Integer.toString(blocksTotal));
        return result;
    }

    private void createSlaveNodeEntities(String url, TopologyEntityParserResult result) throws IOException {
        final String urlString = buildNamenodeInfo(url);
        final Map<String, JMXBean> jmxBeanMap = JMXQueryHelper.query(urlString);
        final JMXBean bean = jmxBeanMap.get(TopologyResourceURLBuilder.HDFS_JMX_NAMENODE_INFO);
        if (bean == null || bean.getPropertyMap() == null) {
            throw new ServiceNotResponseException("Invalid JMX format, NameNodeInfo bean is null!");
        }
        createAllDataNodeEntities(bean, result);
        createAllJournalNodeEntities(bean, result);
    }

    private void createAllJournalNodeEntities(JMXBean bean, TopologyEntityParserResult result) throws UnknownHostException {
        String jnInfoString = (String) bean.getPropertyMap().get(JN_TRANSACTION_INFO);
        JSONObject jsonObject = new JSONObject(jnInfoString);
        long lastTxId = Long.parseLong(jsonObject.getString(LAST_TX_ID));

        String journalnodeString = (String) bean.getPropertyMap().get(JN_STATUS);
        JSONArray jsonArray = new JSONArray(journalnodeString);
        JSONObject jsonMap = (JSONObject) jsonArray.get(0);

        Map<String, JournalNodeServiceAPIEntity> journalNodesMap = new HashMap<>();
        String QJM = jsonMap.getString("manager");
        Pattern qjm = Pattern.compile(QJM_PATTERN);
        Matcher jpmMatcher = qjm.matcher(QJM);

        while (jpmMatcher.find()) {
            JournalNodeServiceAPIEntity entity = new JournalNodeServiceAPIEntity();
            entity.setTags(new HashMap<>());
            String ip = jpmMatcher.group(1);
            entity.getTags().put(TopologyConstants.HOSTNAME_TAG, EntityBuilderHelper.resolveHostByIp(ip));
            entity.setStatus(TopologyConstants.DATA_NODE_DEAD_STATUS);
            journalNodesMap.put(ip, entity);
        }

        String stream = jsonMap.getString("stream");
        Pattern status = Pattern.compile(STATUS_PATTERN);
        Matcher statusMatcher = status.matcher(stream);
        while (statusMatcher.find()) {
            JournalNodeServiceAPIEntity entity = new JournalNodeServiceAPIEntity();
            entity.setTags(new HashMap<>());
            String ip = statusMatcher.group(1);
            if (journalNodesMap.containsKey(ip)) {
                long txid = Long.parseLong(statusMatcher.group(2));
                journalNodesMap.get(ip).setWrittenTxidDeviation(lastTxId - txid);
                journalNodesMap.get(ip).setStatus(TopologyConstants.DATA_NODE_LIVE_STATUS);
            }
        }
        if (!result.getNodes().containsKey(TopologyConstants.JOURNAL_NODE_ROLE)) {
            result.getNodes().put(TopologyConstants.JOURNAL_NODE_ROLE, new ArrayList<>());
        }
        result.getNodes().get(TopologyConstants.JOURNAL_NODE_ROLE).addAll(journalNodesMap.values());

        double value = statusMatcher.groupCount() / journalNodesMap.size();
        result.getMetrics().add(generateMetric(TopologyConstants.JOURNAL_NODE_ROLE, value));
    }

    private void createAllDataNodeEntities(JMXBean bean, TopologyEntityParserResult result) throws JSONException, IOException {
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
            final HdfsServiceTopologyAPIEntity entity = new HdfsServiceTopologyAPIEntity();
            entity.setTags(new HashMap<String, String>());
            entity.getTags().put(TopologyConstants.HOSTNAME_TAG, hostname);
            entity.getTags().put(TopologyConstants.RACK_TAG, EntityBuilderHelper.resolveRackByHost(hostname));
            entity.getTags().put(TopologyConstants.ROLE_TAG, TopologyConstants.DATA_NODE_ROLE);
            if (deadNode.getBoolean(DATA_NODE_DECOMMISSIONED_STATE)) {
                ++numDeadDecommNodes;
                entity.setStatus(TopologyConstants.DATA_NODE_DEAD_DECOMMISSIONED_STATUS);
            } else {
                entity.setStatus(TopologyConstants.DATA_NODE_DEAD_STATUS);
            }
            ++numDeadNodes;
            result.getNodes().get(TopologyConstants.DATA_NODE_ROLE).add(entity);
        }
        LOG.info("Dead nodes " + numDeadNodes + ", dead but decommissioned nodes: " + numDeadDecommNodes);

        String liveNodesStrings = (String) bean.getPropertyMap().get(LIVE_NODES);
        tokener = new JSONTokener(liveNodesStrings);
        jsonNodesObject = new JSONObject(tokener);
        final JSONArray liveNodes = jsonNodesObject.names();
        for (int i = 0; liveNodes != null && i < liveNodes.length(); ++i) {
            final String hostname = liveNodes.getString(i);
            final JSONObject liveNode = jsonNodesObject.getJSONObject(hostname);

            final HdfsServiceTopologyAPIEntity entity = new HdfsServiceTopologyAPIEntity();
            entity.setTags(new HashMap<String, String>());
            entity.getTags().put(TopologyConstants.HOSTNAME_TAG, hostname);
            entity.getTags().put(TopologyConstants.ROLE_TAG, TopologyConstants.DATA_NODE_ROLE);
            final Number configuredCapacity = (Number) liveNode.get(DATA_NODE_CAPACITY);
            entity.setConfiguredCapacityTB(Double.toString(configuredCapacity.doubleValue() / 1024.0 / 1024.0 / 1024.0 / 1024.0));
            final Number capacityUsed = (Number) liveNode.get(DATA_NODE_USED_SPACE);
            entity.setUsedCapacityTB(Double.toString(capacityUsed.doubleValue() / 1024.0 / 1024.0 / 1024.0 / 1024.0));
            final Number blocksTotal = (Number) liveNode.get(DATA_NODE_NUM_BLOCKS);
            entity.setNumBlocks(Double.toString(blocksTotal.doubleValue()));
            final String adminState = liveNode.getString(DATA_NODE_ADMIN_STATE);
            if (DATA_NODE_DECOMMISSIONED.equalsIgnoreCase(adminState)) {
//				entity.setStatus(FeederConstants.DATA_NODE_LIVE_STATUS);
                ++numLiveDecommNodes;
                entity.setStatus(TopologyConstants.DATA_NODE_LIVE_DECOMMISSIONED_STATUS);
            } else {
                entity.setStatus(TopologyConstants.DATA_NODE_LIVE_STATUS);
            }
            numLiveNodes++;
            result.getNodes().get(TopologyConstants.DATA_NODE_ROLE).add(entity);
        }
        LOG.info("Live nodes " + numLiveNodes + ", live but decommissioned nodes: " + numLiveDecommNodes);

        double value = numLiveNodes * 1.0d / result.getNodes().get(TopologyConstants.DATA_NODE_ROLE).size();
        result.getMetrics().add(generateMetric(TopologyConstants.DATA_NODE_ROLE, value));
    }

    private GenericMetricEntity generateMetric(String serviceName, double value) {
        Map<String, String> tags = new HashMap<>();
        tags.put(TopologyConstants.SITE_TAG, site);
        tags.put(TopologyConstants.ROLE_TAG, serviceName);
        String metricName = String.format(METRIC_NAME_FORMAT, serviceName);
        return EntityBuilderHelper.metricWrapper(0L, metricName, value, tags);
    }

    private String buildFSNamesystemURL(String url) {
        return url + "&qry=" + JMX_FS_NAME_SYSTEM_BEAN_NAME;
    }

    private String buildNamenodeInfo(String url) {
        return url + "&qry=" + JMX_NAMENODE_INFO;
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
