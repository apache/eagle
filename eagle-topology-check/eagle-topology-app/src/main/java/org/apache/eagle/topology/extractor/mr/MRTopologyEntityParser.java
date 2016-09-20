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

import org.apache.eagle.app.utils.AppConstants;
import org.apache.eagle.app.utils.PathResolverHelper;
import org.apache.eagle.app.utils.connection.InputStreamUtils;
import org.apache.eagle.topology.TopologyCheckAppConfig;
import org.apache.eagle.topology.TopologyConstants;
import org.apache.eagle.topology.TopologyEntityParserResult;
import org.apache.eagle.topology.entity.MRServiceTopologyAPIEntity;
import org.apache.eagle.topology.extractor.TopologyEntityParser;
import org.apache.eagle.topology.utils.EntityBuilderHelper;
import org.apache.eagle.topology.utils.ServiceNotResponseException;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;

import static org.apache.eagle.topology.TopologyConstants.*;

public class MRTopologyEntityParser implements TopologyEntityParser {

    private String [] rmUrls;
    private String site;

    // class members
    private static final String YARN_NODES_URL = "/ws/v1/cluster/nodes?anonymous=true";

    private static final Logger LOGGER = LoggerFactory.getLogger(MRTopologyEntityParser.class);
    private static final ObjectMapper OBJ_MAPPER = new ObjectMapper();

    static {
        OBJ_MAPPER.configure(JsonParser.Feature.ALLOW_NON_NUMERIC_NUMBERS, true);
    }

    public MRTopologyEntityParser(String site, TopologyCheckAppConfig.MRConfig config) {
        this.site = site;
        this.rmUrls = config.rmUrls;
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
        for (String url : rmUrls) {
            try {
                return doParse(PathResolverHelper.buildUrlPath(url, YARN_NODES_URL), timestamp);
            } catch (ServiceNotResponseException ex) {
                // reSelect url
            }
        }
        return null;
    }

    private TopologyEntityParserResult doParse(String url, long timestamp) throws ServiceNotResponseException {
        final TopologyEntityParserResult result = new TopologyEntityParserResult();
        result.setVersion(TopologyConstants.HadoopVersion.V2);
        result.getNodes().put(TopologyConstants.RESOURCE_MANAGER_ROLE, new ArrayList<>());
        result.getNodes().put(TopologyConstants.NODE_MANAGER_ROLE, new ArrayList<>());
        InputStream is = null;
        try {
            LOGGER.info("Going to query URL: " + url);
            is = InputStreamUtils.getInputStream(url, null, AppConstants.CompressionType.NONE);

            YarnNodeInfoWrapper nodeWrapper = OBJ_MAPPER.readValue(is, YarnNodeInfoWrapper.class);
            if (nodeWrapper.getNodes() == null || nodeWrapper.getNodes().getNode() == null) {
                throw new ServiceNotResponseException("Invalid result of URL: " + url);
            }
            int runningNodeCount = 0;
            int lostNodeCount = 0;
            int unhealthyNodeCount = 0;
            final List<YarnNodeInfo> list = nodeWrapper.getNodes().getNode();
            for (YarnNodeInfo info : list) {
                final MRServiceTopologyAPIEntity nodeManagerEntity = createEntity(NODE_MANAGER_ROLE, info.getNodeHostName(), extractRack(info), timestamp);
                if (info.getHealthReport() != null && (!info.getHealthReport().isEmpty())) {
                    nodeManagerEntity.setHealthReport(info.getHealthReport());
                }
                // TODO: Need to remove the manually mapping RUNNING -> running, LOST - > lost, UNHEALTHY -> unhealthy
                if (info.getState() != null) {
                    final String state = info.getState().toLowerCase();
                    nodeManagerEntity.setStatus(state);
                    if (state.equals(TopologyConstants.NODE_MANAGER_RUNNING_STATUS)) {
                        ++runningNodeCount;
                    } else if (state.equals(TopologyConstants.NODE_MANAGER_LOST_STATUS)) {
                        ++lostNodeCount;
                    } else if (state.equals(TopologyConstants.NODE_MANAGER_UNHEALTHY_STATUS)) {
                        ++unhealthyNodeCount;
                    }
                }
                result.getNodes().get(TopologyConstants.NODE_MANAGER_ROLE).add(nodeManagerEntity);
            }
            LOGGER.info("Running NMs: " + runningNodeCount + ", lost NMs: " + lostNodeCount + ", unhealthy NMs: " + unhealthyNodeCount);
            final MRServiceTopologyAPIEntity resourceManagerEntity = createEntity(TopologyConstants.RESOURCE_MANAGER_ROLE, extractMasterHost(url), null, timestamp);
            resourceManagerEntity.setStatus(TopologyConstants.RESOURCE_MANAGER_ACTIVE_STATUS);
            result.getNodes().get(TopologyConstants.NODE_MANAGER_ROLE).add(resourceManagerEntity);
            double value = runningNodeCount * 1d / list.size();
            result.getMetrics().add(EntityBuilderHelper.generateMetric(TopologyConstants.NODE_MANAGER_ROLE, value, site, timestamp));
            return result;
        } catch (Exception e) {
            //e.printStackTrace();
            throw new ServiceNotResponseException(e);
        } finally {
            if (is != null) {
                try {
                    is.close();
                } catch (IOException e) {
                    e.printStackTrace();
                    // Do nothing
                }
            }
        }
    }

    private String extractMasterHost(String url) {
        Matcher matcher = TopologyConstants.HTTP_HOST_MATCH_PATTERN.matcher(url);
        if (matcher.find()) {
            return matcher.group(1);
        }
        return url;
    }

    private String extractRack(YarnNodeInfo info) {
        if (info.getRack() == null) {
            return null;
        }
        String value = info.getRack();
        value = value.substring(value.lastIndexOf('/') + 1);
        return value;
    }

    private MRServiceTopologyAPIEntity createEntity(String roleType, String hostname, String rack, long updateTime) {
        MRServiceTopologyAPIEntity entity = new MRServiceTopologyAPIEntity();
        entity.setTimestamp(updateTime);
        Map<String, String> tags = new HashMap<String, String>();
        entity.setTags(tags);
        tags.put(SITE_TAG, site);
        tags.put(ROLE_TAG, roleType);
        tags.put(HOSTNAME_TAG, hostname);
        if (rack == null) {
            rack = EntityBuilderHelper.resolveRackByHost(hostname);
        }
        tags.put(RACK_TAG, rack);
        return entity;
    }

}
