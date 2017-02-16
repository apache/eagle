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

package org.apache.eagle.service.topology.resource.impl;

import org.apache.eagle.alert.coordination.model.internal.Topology;
import org.apache.eagle.alert.engine.UnitTopologyMain;
import org.apache.eagle.alert.engine.coordinator.StreamingCluster;
import org.apache.eagle.alert.engine.runner.UnitTopologyRunner;
import org.apache.eagle.alert.metadata.IMetadataDao;
import org.apache.eagle.alert.metadata.impl.MetadataDaoFactory;
import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.Nimbus;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.generated.TopologySummary;
import org.apache.storm.utils.NimbusClient;
import org.apache.storm.utils.Utils;
import com.typesafe.config.ConfigFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;


public class TopologyMgmtResourceImpl {
    private static final IMetadataDao dao = MetadataDaoFactory.getInstance().getMetadataDao();
    @SuppressWarnings("unused")
    private static final Logger LOG = LoggerFactory.getLogger(TopologyMgmtResourceImpl.class);

    private static final String DEFAULT_NIMBUS_HOST = "sandbox.hortonworks.com";
    private static final Integer DEFAULT_NIMBUS_THRIFT_PORT = 6627;
    private static final String STORM_JAR_PATH = "topology.stormJarPath";


    @SuppressWarnings( {"rawtypes", "unchecked"})
    private Map getStormConf(List<StreamingCluster> clusters, String clusterId) throws Exception {
        Map<String, Object> stormConf = Utils.readStormConfig();
        if (clusterId == null) {
            // TODO: change to NIMBUS_SEEDS list in EAGLE-907
            stormConf.put(Config.NIMBUS_HOST, DEFAULT_NIMBUS_HOST);
            stormConf.put(Config.NIMBUS_THRIFT_PORT, DEFAULT_NIMBUS_THRIFT_PORT);
        } else {
            if (clusters == null) {
                clusters = dao.listClusters();
            }
            Optional<StreamingCluster> scOp = TopologyMgmtResourceHelper.findById(clusters, clusterId);
            StreamingCluster cluster;
            if (scOp.isPresent()) {
                cluster = scOp.get();
            } else {
                throw new Exception("Fail to find cluster: " + clusterId);
            }
            // TODO: change to NIMBUS_SEEDS list in EAGLE-907
            stormConf.put(Config.NIMBUS_HOST, cluster.getDeployments().getOrDefault(StreamingCluster.NIMBUS_HOST, DEFAULT_NIMBUS_HOST));
            stormConf.put(Config.NIMBUS_THRIFT_PORT, Integer.valueOf(cluster.getDeployments().get(StreamingCluster.NIMBUS_THRIFT_PORT)));
        }
        return stormConf;
    }

    private void createTopologyHelper(Topology topologyDef, com.typesafe.config.Config config) {
        int numOfSpoutTasks = config.getInt(UnitTopologyRunner.SPOUT_TASK_NUM);
        int numOfRouterBolts = config.getInt(UnitTopologyRunner.ROUTER_TASK_NUM);
        int numOfAlertBolts = config.getInt(UnitTopologyRunner.ALERT_TASK_NUM);
        int numOfPublishTasks = config.getInt(UnitTopologyRunner.PUBLISH_TASK_NUM);
        topologyDef.setSpoutId(UnitTopologyRunner.spoutName);
        topologyDef.setPubBoltId(UnitTopologyRunner.alertPublishBoltName);
        topologyDef.setNumOfSpout(numOfSpoutTasks);
        topologyDef.setNumOfGroupBolt(numOfRouterBolts);
        topologyDef.setNumOfAlertBolt(numOfAlertBolts);
        topologyDef.setNumOfPublishBolt(numOfPublishTasks);
        dao.addTopology(topologyDef);
    }

    private StormTopology createTopology(Topology topologyDef) {
        com.typesafe.config.Config topologyConf = ConfigFactory.load("topology-sample-definition.conf");
        String stormJarPath = "";
        if (topologyConf.hasPath(STORM_JAR_PATH)) {
            stormJarPath = topologyConf.getString(STORM_JAR_PATH);
        }
        System.setProperty("storm.jar", stormJarPath);
        createTopologyHelper(topologyDef, topologyConf);
        return UnitTopologyMain.createTopology(topologyConf);
    }

    public void startTopology(String topologyName) throws Exception {
        Optional<Topology> tdop = TopologyMgmtResourceHelper.findById(dao.listTopologies(), topologyName);
        Topology topologyDef;
        if (tdop.isPresent()) {
            topologyDef = tdop.get();
        } else {
            topologyDef = new Topology();
            topologyDef.setName(topologyName);
        }
        StormSubmitter.submitTopology(topologyName, getStormConf(null, topologyDef.getClusterName()), createTopology(topologyDef));
    }

    public void stopTopology(String topologyName) throws Exception {
        Optional<Topology> tdop = TopologyMgmtResourceHelper.findById(dao.listTopologies(), topologyName);
        Topology topologyDef;
        if (tdop.isPresent()) {
            topologyDef = tdop.get();
        } else {
            throw new Exception("Fail to find topology " + topologyName);
        }
        Nimbus.Client stormClient = NimbusClient.getConfiguredClient(getStormConf(null, topologyDef.getClusterName())).getClient();
        stormClient.killTopology(topologyName);
    }

    @SuppressWarnings( {"rawtypes", "unused"})
    private TopologySummary getTopologySummery(List<StreamingCluster> clusters, Topology topologyDef) throws Exception {
        Map stormConf = getStormConf(clusters, topologyDef.getClusterName());
        Nimbus.Client stormClient = NimbusClient.getConfiguredClient(stormConf).getClient();
        Optional<TopologySummary> tOp = stormClient.getClusterInfo().get_topologies().stream().filter(topology -> topology.get_name().equalsIgnoreCase(topologyDef.getName())).findFirst();
        if (tOp.isPresent()) {
            String id = tOp.get().get_id();
            //StormTopology stormTopology= stormClient.getTopology(id);
            return tOp.get();
        } else {
            return null;
        }
    }

    public List<TopologyStatus> getTopologies() throws Exception {
        List<Topology> topologyDefinitions = dao.listTopologies();
        List<StreamingCluster> clusters = dao.listClusters();

        List<TopologyStatus> topologies = new ArrayList<>();
        for (Topology topologyDef : topologyDefinitions) {
            TopologySummary topologySummary = getTopologySummery(clusters, topologyDef);
            if (topologySummary != null) {
                TopologyStatus t = new TopologyStatus();
                t.setName(topologySummary.get_name());
                t.setId(topologySummary.get_id());
                t.setState(topologySummary.get_status());
                t.setTopology(topologyDef);
                topologies.add(t);
            }
        }
        return topologies;
    }

}
