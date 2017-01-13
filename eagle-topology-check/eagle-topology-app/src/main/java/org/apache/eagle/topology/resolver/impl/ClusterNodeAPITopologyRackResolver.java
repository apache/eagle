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

package org.apache.eagle.topology.resolver.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.eagle.app.utils.AppConstants;
import org.apache.eagle.app.utils.connection.InputStreamUtils;
import org.apache.eagle.topology.TopologyCheckAppConfig;
import org.apache.eagle.topology.resolver.TopologyRackResolver;
import org.apache.eagle.topology.resolver.model.Node;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;

/**
 * resolve rack by ClusterNode API.
 * https://hadoop.apache.org/docs/r2.6.0/hadoop-yarn/hadoop-yarn-site/ResourceManagerRest.html
 */
public class ClusterNodeAPITopologyRackResolver implements TopologyRackResolver {

    private static final Logger LOG = LoggerFactory.getLogger(ClusterNodeAPITopologyRackResolver.class);
    private static final String DEFAULT_RACK_NAME = "/default-rack";
    private String activeApiUrl = "";
    private String hostPort = "8041";//TODO configurable
    private static final ObjectMapper OBJ_MAPPER = new ObjectMapper();

    @Override
    public void prepare(TopologyCheckAppConfig config) {
        this.activeApiUrl = config.getConfig().getString("topology.resolverAPIUrl");
    }

    @Override
    public String resolve(String hostname) {
        String nodeid = hostname + ":" + hostPort;
        String requestUrl = activeApiUrl + "/" + nodeid;
        String rack = DEFAULT_RACK_NAME;
        InputStream is = null;
        try {
            is = InputStreamUtils.getInputStream(requestUrl, null, AppConstants.CompressionType.NONE);
            LOG.info("resolve rack by api url {}", requestUrl);
            Node node = OBJ_MAPPER.readValue(is, Node.class);
            rack = node.getNode().getRack();
        } catch (Exception e) {
            LOG.warn("resolve rack by api url {} failed, {}", requestUrl, e);
            return rack;
        } finally {
            if (is != null) {
                try {
                    is.close();
                } catch (Exception e) {
                    LOG.warn("{}", e);
                }
            }
        }
        return rack;
    }
}
