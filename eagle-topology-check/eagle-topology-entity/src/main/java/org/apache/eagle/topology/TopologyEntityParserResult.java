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

import org.apache.eagle.topology.entity.TopologyBaseAPIEntity;

import java.util.SortedMap;
import java.util.TreeMap;

public class TopologyEntityParserResult {
    private TopologyConstants.HadoopVersion version;
    private final SortedMap<String, TopologyBaseAPIEntity> masterNodes = new TreeMap<String, TopologyBaseAPIEntity>();
    private final SortedMap<String, TopologyBaseAPIEntity> slaveNodes = new TreeMap<String, TopologyBaseAPIEntity>();

    public TopologyConstants.HadoopVersion getVersion() {
        return version;
    }
    public void setVersion(TopologyConstants.HadoopVersion version) {
        this.version = version;
    }
    public SortedMap<String, TopologyBaseAPIEntity> getMasterNodes() {
        return masterNodes;
    }
    public SortedMap<String, TopologyBaseAPIEntity> getSlaveNodes() {
        return slaveNodes;
    }
}
