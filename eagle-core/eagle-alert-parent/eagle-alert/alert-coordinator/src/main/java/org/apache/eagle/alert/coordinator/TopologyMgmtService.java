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
package org.apache.eagle.alert.coordinator;

import static org.apache.eagle.alert.coordinator.CoordinatorConstants.CONFIG_ITEM_COORDINATOR;
import static org.apache.eagle.alert.coordinator.CoordinatorConstants.NUM_OF_ALERT_BOLTS_PER_TOPOLOGY;

import org.apache.eagle.alert.coordination.model.internal.Topology;
import org.apache.eagle.alert.coordinator.model.TopologyUsage;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.util.Collections;
import java.util.List;

/**
 * @since Mar 29, 2016.
 */
public class TopologyMgmtService {

    public static class TopologyMeta {
        public String topologyId;
        public Topology topology;
        public TopologyUsage usage;

        public String clusterId;
        public String nimbusHost;
        public String nimbusPort;

    }

    public static class StormClusterMeta {
        public String clusterId;
        public String nimbusHost;
        public String nimbusPort;
        public String stormVersion;
    }

    @SuppressWarnings("unused")
    private int boltParallelism = 0;
    private int numberOfBoltsPerTopology = 0;

    public TopologyMgmtService() {
        Config config = ConfigFactory.load().getConfig(CONFIG_ITEM_COORDINATOR);
        boltParallelism = config.getInt(CoordinatorConstants.BOLT_PARALLELISM);
        numberOfBoltsPerTopology = config.getInt(NUM_OF_ALERT_BOLTS_PER_TOPOLOGY);
    }

    public int getNumberOfAlertBoltsInTopology() {
        return numberOfBoltsPerTopology;
    }

    /**
     * TODO: call topology mgmt API to create a topology.
     */
    public TopologyMeta creatTopology() {
        // TODO
        throw new UnsupportedOperationException("not supported yet!");
    }

    public List<TopologyMeta> listTopologies() {
        // TODO
        return Collections.emptyList();
    }
}
