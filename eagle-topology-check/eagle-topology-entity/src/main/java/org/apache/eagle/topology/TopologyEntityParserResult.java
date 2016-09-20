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

import org.apache.eagle.log.entity.GenericMetricEntity;
import org.apache.eagle.topology.entity.TopologyBaseAPIEntity;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TopologyEntityParserResult {
    private TopologyConstants.HadoopVersion version;
    private Map<String, List<TopologyBaseAPIEntity>> nodes = new HashMap<>();
    private List<GenericMetricEntity> metrics = new ArrayList<>();

    public List<GenericMetricEntity> getMetrics() {
        return metrics;
    }

    public void setMetrics(List<GenericMetricEntity> metrics) {
        this.metrics = metrics;
    }

    public void setNodes(Map<String, List<TopologyBaseAPIEntity>> nodes) {
        this.nodes = nodes;
    }
    public Map<String, List<TopologyBaseAPIEntity>> getNodes() {
        return nodes;
    }

    public TopologyConstants.HadoopVersion getVersion() {
        return version;
    }
    public void setVersion(TopologyConstants.HadoopVersion version) {
        this.version = version;
    }

}
