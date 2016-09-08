/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one or more
 *  * contributor license agreements.  See the NOTICE file distributed with
 *  * this work for additional information regarding copyright ownership.
 *  * The ASF licenses this file to You under the Apache License, Version 2.0
 *  * (the "License"); you may not use this file except in compliance with
 *  * the License.  You may obtain a copy of the License at
 *  *
 *  *    http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package org.apache.eagle.alert.engine.topology;

import java.io.Serializable;
import java.util.HashMap;

import org.apache.eagle.alert.engine.spout.CorrelationSpout;
import org.apache.eagle.alert.engine.spout.CreateTopicUtils;
import org.apache.eagle.alert.utils.AlertConstants;
import org.apache.eagle.alert.utils.StreamIdConversion;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.LocalCluster;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.BoltDeclarer;
import backtype.storm.topology.SpoutDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

/**
 * Since 4/28/16.
 */
@SuppressWarnings( {"serial", "unused", "rawtypes"})
public class CoordinatorSpoutIntegrationTest implements Serializable {
    private static final Logger LOG = LoggerFactory.getLogger(CoordinatorSpoutIntegrationTest.class);

    @Ignore  // this test need zookeeper
    @Test
    public void testConfigNotify() throws Exception {
        final String topoId = "myTopology";
        int numGroupbyBolts = 2;
        int numTotalGroupbyBolts = 3;
        System.setProperty("correlation.serviceHost", "sandbox.hortonworks.com");
        System.setProperty("correlation.servicePort", "58080");
        System.setProperty("withMetadataChangeNotifyService.zkConfig.zkQuorum", "sandbox.hortonworks.com:2181");
        System.setProperty("correlation.numGroupbyBolts", String.valueOf(numGroupbyBolts));
        System.setProperty("correlation.topologyName", topoId);
        System.setProperty("correlation.mode", "local");
        System.setProperty("correlation.zkHosts", "sandbox.hortonworks.com:2181");
        final String topicName1 = "testTopic3";
        final String topicName2 = "testTopic4";
        // ensure topic ready
        LogManager.getLogger(CorrelationSpout.class).setLevel(Level.DEBUG);
        Config config = ConfigFactory.load();

        CreateTopicUtils.ensureTopicReady(System.getProperty("withMetadataChangeNotifyService.zkConfig.zkQuorum"), topicName1);
        CreateTopicUtils.ensureTopicReady(System.getProperty("withMetadataChangeNotifyService.zkConfig.zkQuorum"), topicName2);

        TopologyBuilder topoBuilder = new TopologyBuilder();

        int numBolts = config.getInt("correlation.numGroupbyBolts");
        String spoutId = "correlation-spout";
        CorrelationSpout spout = new CorrelationSpout(config, topoId,
            new MockMetadataChangeNotifyService(topoId, spoutId), numBolts);
        SpoutDeclarer declarer = topoBuilder.setSpout(spoutId, spout);
        declarer.setNumTasks(2);
        for (int i = 0; i < numBolts; i++) {
            TestBolt bolt = new TestBolt();
            BoltDeclarer boltDecl = topoBuilder.setBolt("engineBolt" + i, bolt);
            boltDecl.fieldsGrouping(spoutId,
                StreamIdConversion.generateStreamIdBetween(AlertConstants.DEFAULT_SPOUT_NAME, AlertConstants.DEFAULT_ROUTERBOLT_NAME + i), new Fields());
        }

        String topoName = config.getString("correlation.topologyName");
        LOG.info("start topology in local mode");
        LocalCluster cluster = new LocalCluster();
        StormTopology topology = topoBuilder.createTopology();
        cluster.submitTopology(topoName, new HashMap(), topology);


        Utils.sleep(Long.MAX_VALUE);
    }


}
