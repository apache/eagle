/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.alert.coordinator.mock;

import java.util.List;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.eagle.alert.coordination.model.internal.Topology;
import org.apache.eagle.alert.coordinator.TopologyMgmtService;
import org.apache.eagle.alert.coordinator.model.AlertBoltUsage;
import org.apache.eagle.alert.coordinator.model.GroupBoltUsage;
import org.apache.eagle.alert.coordinator.model.TopologyUsage;

public class TestTopologyMgmtService extends TopologyMgmtService {

    public static int BOLT_NUMBER = 5;
    
    private int i = 0;

    @Override
    public TopologyMeta creatTopology() {
        TopologyMeta tm = new TopologyMeta();
        tm.topologyId = "Topoloy" + (i++);
        tm.clusterId = "default-cluster";
        tm.nimbusHost = "localhost";
        tm.nimbusPort = "3000";
        Pair<Topology, TopologyUsage> pair = TestTopologyMgmtService.createEmptyTopology(tm.topologyId);
        tm.topology = pair.getLeft();
        tm.usage = pair.getRight();
        
        return tm;
    }

    @Override
    public List<TopologyMeta> listTopologies() {
        // TODO Auto-generated method stub
        return super.listTopologies();
    }

    public static Pair<Topology, TopologyUsage> createEmptyTopology(String topoName) {
        Topology t = new Topology(topoName, TestTopologyMgmtService.BOLT_NUMBER, TestTopologyMgmtService.BOLT_NUMBER);
        for (int i = 0; i < t.getNumOfGroupBolt(); i++) {
            t.getGroupNodeIds().add(t.getName() + "-grp-" + i);
        }
        for (int i = 0; i < t.getNumOfAlertBolt(); i++) {
            t.getAlertBoltIds().add(t.getName() + "-alert-" + i);
        }
    
        TopologyUsage u = new TopologyUsage(topoName);
        for (String gnid : t.getGroupNodeIds()) {
            u.getGroupUsages().put(gnid, new GroupBoltUsage(gnid));
        }
        for (String anid : t.getAlertBoltIds()) {
            u.getAlertUsages().put(anid, new AlertBoltUsage(anid));
        }
    
        return Pair.of(t, u);
    }
}