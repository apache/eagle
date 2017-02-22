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

import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.eagle.alert.coordination.model.internal.Topology;
import org.apache.eagle.alert.coordinator.TopologyMgmtService;
import org.apache.eagle.alert.coordinator.model.AlertBoltUsage;
import org.apache.eagle.alert.coordinator.model.GroupBoltUsage;
import org.apache.eagle.alert.coordinator.model.TopologyUsage;

@org.junit.Ignore
public class TestTopologyMgmtService extends TopologyMgmtService {

    public static final int ROUTER_BOLT_NUMBER = 6;
    public static final int BOLT_NUMBER = 7;

    private int boltNumber;
    private int routerNumber;
    private int i = 0;
    private String namePrefix = "Topology";

    // a config used to check if createTopology is enabled. FIXME: another class of mgmt service might be better
    private boolean enableCreateTopology = false;

    public TestTopologyMgmtService() {
        boltNumber = BOLT_NUMBER;// default behaivor
        this.routerNumber = ROUTER_BOLT_NUMBER;
    }

    public TestTopologyMgmtService(int routerNumber, int boltNumber) {
        this.routerNumber = routerNumber;
        this.boltNumber = boltNumber;
    }

    public TestTopologyMgmtService(int routerNumber, int boltNumber, String prefix, boolean enable) {
        this.routerNumber = routerNumber;
        this.boltNumber = boltNumber;
        this.namePrefix = prefix;
        this.enableCreateTopology = enable;
    }

    @Override
    public TopologyMeta creatTopology() {
        if (enableCreateTopology) {
            TopologyMeta tm = new TopologyMeta();
            tm.topologyId = namePrefix + (i++);
            tm.clusterId = "default-cluster";
            tm.nimbusSeeds = Arrays.asList("localhost");
            tm.nimbusPort = "3000";
            Pair<Topology, TopologyUsage> pair = createEmptyTopology(tm.topologyId);
            tm.topology = pair.getLeft();
            tm.usage = pair.getRight();
            return tm;
        } else {
            throw new UnsupportedOperationException("not supported yet!");
        }
    }

    @Override
    public List<TopologyMeta> listTopologies() {
        return super.listTopologies();
    }

    public Pair<Topology, TopologyUsage> createEmptyTopology(String topoName) {
        Topology t = new Topology(topoName, routerNumber, boltNumber);
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