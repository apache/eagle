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

package org.apache.eagle.topology.extractor.mr;

import org.apache.eagle.app.utils.PathResolverHelper;
import org.apache.eagle.topology.TopologyCheckAppConfig;
import org.apache.eagle.topology.TopologyConstants;
import org.apache.eagle.topology.extractor.TopologyEntityParserResult;
import org.apache.eagle.topology.entity.MRServiceTopologyAPIEntity;
import org.apache.eagle.topology.extractor.TopologyEntityParser;
import org.apache.eagle.topology.resolver.TopologyRackResolver;
import org.apache.eagle.topology.utils.EntityBuilderHelper;
import org.apache.eagle.app.utils.connection.ServiceNotResponseException;
import org.apache.eagle.app.utils.connection.URLResourceFetcher;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;

import static org.apache.eagle.topology.TopologyConstants.*;
import static org.apache.eagle.topology.utils.EntityBuilderHelper.generateKey;

public class MRTopologyEntityParser implements TopologyEntityParser {

    private String[] rmUrls;
    private String historyServerUrl;
    private String site;
    private TopologyRackResolver rackResolver;

    private static final String YARN_NODES_URL = "/ws/v1/cluster/nodes?anonymous=true";
    private static final String YARN_HISTORY_SERVER_URL = "/ws/v1/history/info";

    private static final Logger LOGGER = LoggerFactory.getLogger(MRTopologyEntityParser.class);
    private static final ObjectMapper OBJ_MAPPER = new ObjectMapper();

    static {
        OBJ_MAPPER.configure(JsonParser.Feature.ALLOW_NON_NUMERIC_NUMBERS, true);
    }

    public MRTopologyEntityParser(String site, TopologyCheckAppConfig.MRConfig config, TopologyRackResolver rackResolver) {
        this.site = site;
        this.rmUrls = config.rmUrls;
        this.historyServerUrl = config.historyServerUrl;
        this.rackResolver = rackResolver;
    }

    @Override
    public TopologyConstants.HadoopVersion getHadoopVersion() {
        return TopologyConstants.HadoopVersion.V2;
    }

    @Override
    public TopologyConstants.TopologyType getTopologyType() {
        return TopologyConstants.TopologyType.MR;
    }

    @Override
    public TopologyEntityParserResult parse(long timestamp) {
        final TopologyEntityParserResult result = new TopologyEntityParserResult();

        String rmStatus;
        int inActiveHosts = 0;
        boolean isSuccess = false;
        for (String url : rmUrls) {
            MRServiceTopologyAPIEntity resourceManagerEntity = createEntity(TopologyConstants.RESOURCE_MANAGER_ROLE,
                    extractMasterHost(url), timestamp);
            rmStatus = RESOURCE_MANAGER_ACTIVE_STATUS;
            try {
                InputStream is = URLResourceFetcher.openURLStream(PathResolverHelper.buildUrlPath(url, YARN_NODES_URL));
                if (!isSuccess) {
                    isSuccess = doParse(timestamp, is, result);
                }
            } catch (IOException e) {
                inActiveHosts++;
                LOGGER.warn(e.getMessage(), e);
                rmStatus = RESOURCE_MANAGER_INACTIVE_STATUS;
            } catch (Exception ex) {
                LOGGER.error("fail to parse url {} due to {}, and will cancel this parsing", url, ex.getMessage(), ex);
                result.getSlaveNodes().clear();
            }
            resourceManagerEntity.setStatus(rmStatus);
            result.getMasterNodes().add(resourceManagerEntity);
        }
        double value = (rmUrls.length - inActiveHosts) * 1.0 / rmUrls.length;
        result.getMetrics().add(EntityBuilderHelper.generateMetric(TopologyConstants.RESOURCE_MANAGER_ROLE, value, site, timestamp));

        doCheckHistoryServer(timestamp, result);
        return result;
    }

    private void doCheckHistoryServer(long updateTime, TopologyEntityParserResult result) {
        if (historyServerUrl == null || historyServerUrl.isEmpty()) {
            return;
        }
        String url = PathResolverHelper.buildUrlPath(historyServerUrl, YARN_HISTORY_SERVER_URL);
        double activeHosts = 0;
        InputStream is = null;
        try {
            is = URLResourceFetcher.openURLStream(url);
            activeHosts++;
        } catch (ServiceNotResponseException e) {
            LOGGER.error(e.getMessage(), e);
        } finally {
            URLResourceFetcher.closeInputStream(is);
        }
        result.getMetrics().add(EntityBuilderHelper.generateMetric(TopologyConstants.HISTORY_SERVER_ROLE, activeHosts, site, updateTime));
    }

    private boolean doParse(long timestamp, InputStream is, TopologyEntityParserResult result) throws IOException {
        boolean isSuccess = false;
        String nodeKey;
        Map<String, MRServiceTopologyAPIEntity> nmMap = new HashMap<>();
        Map<String, Integer> statusCount = new HashMap<>();
        try {
            YarnNodeInfoWrapper nodeWrapper = OBJ_MAPPER.readValue(is, YarnNodeInfoWrapper.class);
            int rackWarningCount = 0;
            final List<YarnNodeInfo> list = nodeWrapper.getNodes().getNode();
            for (YarnNodeInfo info : list) {
                final MRServiceTopologyAPIEntity nodeManagerEntity = createEntity(NODE_MANAGER_ROLE, info.getNodeHostName(), timestamp);
                if (!extractRack(info).equalsIgnoreCase(nodeManagerEntity.getTags().get(RACK_TAG)) && rackWarningCount < 10) {
                    LOGGER.warn("rack info is inconsistent, please configure the right rack resolver class");
                    rackWarningCount++;
                }
                nodeManagerEntity.setLastHealthUpdate(info.getLastHealthUpdate());
                if (info.getHealthReport() != null && (!info.getHealthReport().isEmpty())) {
                    nodeManagerEntity.setHealthReport(info.getHealthReport());
                }
                if (info.getState() != null) {
                    String state = info.getState().toLowerCase();
                    nodeManagerEntity.setStatus(state);
                } else {
                    String state = "null";
                    nodeManagerEntity.setStatus(state);
                }

                nodeKey = generateKey(nodeManagerEntity);
                if (nmMap.containsKey(nodeKey)) {
                    if (nmMap.get(nodeKey).getLastUpdateTime() < nodeManagerEntity.getLastHealthUpdate()) {
                        updateStatusCount(statusCount, nmMap.get(nodeKey).getStatus(), -1);
                        nmMap.put(nodeKey, nodeManagerEntity);
                        updateStatusCount(statusCount, nodeManagerEntity.getStatus(), 1);
                    }
                } else {
                    nmMap.put(nodeKey, nodeManagerEntity);
                    updateStatusCount(statusCount, nodeManagerEntity.getStatus(), 1);
                }
            }
            LOGGER.info("Total NMs: {}, Actual NMs: {}, Details: {}", list.size(), nmMap.size(), statusCount);

            double value = statusCount.get(NODE_MANAGER_RUNNING_STATUS) * 1d / nmMap.size();
            result.getMetrics().add(EntityBuilderHelper.generateMetric(TopologyConstants.NODE_MANAGER_ROLE, value, site, timestamp));
            result.getSlaveNodes().addAll(nmMap.values());
            isSuccess = true;
        } finally {
            URLResourceFetcher.closeInputStream(is);
        }
        return isSuccess;
    }

    private void updateStatusCount(Map<String, Integer> statusCount, String status, int increment) {
        if (!statusCount.containsKey(status)) {
            statusCount.put(status, 0);
        }
        statusCount.put(status, statusCount.get(status) + increment);
    }

    private String extractMasterHost(String url) {
        Matcher matcher = TopologyConstants.HTTP_HOST_MATCH_PATTERN.matcher(url);
        if (matcher.find()) {
            return matcher.group(1);
        }
        return url;
    }

    private String extractRack(YarnNodeInfo info) {
        if (info.getRack() == null) {  // if a host is DECOMMISSIONED, then no rack info
            return rackResolver.resolve(info.getNodeHostName());
        }
        String value = info.getRack();
        value = value.substring(value.lastIndexOf('/') + 1);
        return value;
    }


    private MRServiceTopologyAPIEntity createEntity(String roleType, String hostname, long updateTime) {
        MRServiceTopologyAPIEntity entity = new MRServiceTopologyAPIEntity();
        entity.setTimestamp(updateTime);
        entity.setLastUpdateTime(updateTime);
        Map<String, String> tags = new HashMap<String, String>();
        entity.setTags(tags);
        tags.put(SITE_TAG, site);
        tags.put(ROLE_TAG, roleType);
        tags.put(HOSTNAME_TAG, hostname);
        String resolvedRack = rackResolver.resolve(hostname);
        tags.put(RACK_TAG, resolvedRack);
        return entity;
    }

}
