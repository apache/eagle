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

package org.apache.eagle.alert.engine.runner;

import java.util.ArrayList;
import java.util.List;

import org.apache.eagle.alert.engine.coordinator.IMetadataChangeNotifyService;
import org.apache.eagle.alert.engine.evaluator.impl.PolicyGroupEvaluatorImpl;
import org.apache.eagle.alert.engine.publisher.impl.AlertPublisherImpl;
import org.apache.eagle.alert.engine.router.impl.StreamRouterImpl;
import org.apache.eagle.alert.engine.spout.CorrelationSpout;
import org.apache.eagle.alert.utils.AlertConstants;
import org.apache.eagle.alert.utils.StreamIdConversion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.BoltDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigRenderOptions;

/**
 * By default
 * 1. one spout with multiple tasks
 * 2. multiple router bolts with each bolt having exactly one task
 * 3. multiple alert bolts with each bolt having exactly one task
 * 4. one publish bolt with multiple tasks
 */
public class UnitTopologyRunner {
    private static final Logger LOG = LoggerFactory.getLogger(UnitTopologyRunner.class);
    public final static String spoutName = "alertEngineSpout";
    private final static String streamRouterBoltNamePrefix = "streamRouterBolt";
    private final static String alertBoltNamePrefix = "alertBolt";
    public final static String alertPublishBoltName = "alertPublishBolt";

    public final static String TOTAL_WORKER_NUM = "topology.numOfTotalWorkers";
    public final static String SPOUT_TASK_NUM = "topology.numOfSpoutTasks";
    public final static String ROUTER_TASK_NUM = "topology.numOfRouterBolts";
    public final static String ALERT_TASK_NUM = "topology.numOfAlertBolts";
    public final static String PUBLISH_TASK_NUM = "topology.numOfPublishTasks";
    public final static String LOCAL_MODE = "topology.localMode";
    public final static String MESSAGE_TIMEOUT_SECS = "topology.messageTimeoutSecs";
    public final static int DEFAULT_MESSAGE_TIMEOUT_SECS = 3600;

    private final IMetadataChangeNotifyService metadataChangeNotifyService;

    public UnitTopologyRunner(IMetadataChangeNotifyService metadataChangeNotifyService){
        this.metadataChangeNotifyService = metadataChangeNotifyService;
    }

    public StormTopology buildTopology(String topologyId,
                              int numOfSpoutTasks,
                              int numOfRouterBolts,
                              int numOfAlertBolts,
                              int numOfPublishTasks,
                              Config config) {

        StreamRouterImpl[] routers = new StreamRouterImpl[numOfRouterBolts];
        StreamRouterBolt[] routerBolts = new StreamRouterBolt[numOfRouterBolts];
        PolicyGroupEvaluatorImpl[] evaluators = new PolicyGroupEvaluatorImpl[numOfAlertBolts];
        AlertBolt[] alertBolts = new AlertBolt[numOfAlertBolts];
        AlertPublisherImpl publisher;
        AlertPublisherBolt publisherBolt;

        TopologyBuilder builder = new TopologyBuilder();


        // construct Spout object
        CorrelationSpout spout = new CorrelationSpout(config, topologyId, getMetadataChangeNotifyService(), numOfRouterBolts, spoutName, streamRouterBoltNamePrefix);
        builder.setSpout(spoutName, spout, numOfSpoutTasks).setNumTasks(numOfSpoutTasks);

        // construct StreamRouterBolt objects
        for(int i=0; i<numOfRouterBolts; i++){
            routers[i] = new StreamRouterImpl(streamRouterBoltNamePrefix + i);
            routerBolts[i] = new StreamRouterBolt(routers[i], config, getMetadataChangeNotifyService());
        }

        // construct AlertBolt objects
        for(int i=0; i<numOfAlertBolts; i++){
            evaluators[i] = new PolicyGroupEvaluatorImpl(alertBoltNamePrefix + i);
            alertBolts[i] = new AlertBolt(alertBoltNamePrefix+i, evaluators[i], config, getMetadataChangeNotifyService());
        }

        // construct AlertPublishBolt object
        publisher = new AlertPublisherImpl(alertPublishBoltName);
        publisherBolt = new AlertPublisherBolt(publisher, config, getMetadataChangeNotifyService());

        // connect spout and router bolt, also define output streams for downstreaming alert bolt
        for(int i=0; i<numOfRouterBolts; i++){
            String boltName = streamRouterBoltNamePrefix + i;

            // define output streams, which are based on
            String streamId = StreamIdConversion.generateStreamIdBetween(spoutName, boltName);
            List<String> outputStreamIds = new ArrayList<>(numOfAlertBolts);
            for(int j=0; j<numOfAlertBolts; j++){
                String sid = StreamIdConversion.generateStreamIdBetween(boltName, alertBoltNamePrefix+j);
                outputStreamIds.add(sid);
            }
            routerBolts[i].declareOutputStreams(outputStreamIds);

            /**
             * TODO potentially one route bolt may have multiple tasks, so that is field grouping by groupby fields
             * that means we need a separate field to become groupby field
             */
            builder.setBolt(boltName, routerBolts[i]).fieldsGrouping(spoutName, streamId, new Fields());
        }

        // connect router bolt and alert bolt, also define output streams for downstreaming alert publish bolt
        for(int i=0; i<numOfAlertBolts; i++){
            String boltName = alertBoltNamePrefix + i;
            BoltDeclarer boltDeclarer = builder.setBolt(boltName, alertBolts[i]);
            for(int j=0; j<numOfRouterBolts; j++) {
                String streamId = StreamIdConversion.generateStreamIdBetween(streamRouterBoltNamePrefix+j, boltName);
                boltDeclarer.fieldsGrouping(streamRouterBoltNamePrefix+j, streamId, new Fields());
            }
        }

        // connect alert bolt and alert publish bolt, this is the last bolt in the pipeline
        BoltDeclarer boltDeclarer = builder.setBolt(alertPublishBoltName, publisherBolt).setNumTasks(numOfPublishTasks);
        for(int i=0; i<numOfAlertBolts; i++) {
            boltDeclarer.fieldsGrouping(alertBoltNamePrefix+i, new Fields(AlertConstants.FIELD_0));
        }

        return builder.createTopology();
    }

    public void run(String topologyId,
                    int numOfTotalWorkers,
                    int numOfSpoutTasks,
                    int numOfRouterBolts,
                    int numOfAlertBolts,
                    int numOfPublishTasks,
                    Config config,
                    boolean localMode) {
        backtype.storm.Config stormConfig = new backtype.storm.Config();
        // TODO: Configurable metric consumer instance number

        int messageTimeoutSecs = config.hasPath(MESSAGE_TIMEOUT_SECS)?config.getInt(MESSAGE_TIMEOUT_SECS) : DEFAULT_MESSAGE_TIMEOUT_SECS;
        LOG.info("Set topology.message.timeout.secs as {}",messageTimeoutSecs);
        stormConfig.setMessageTimeoutSecs(messageTimeoutSecs);

        if(config.hasPath("metric")) {
            stormConfig.registerMetricsConsumer(StormMetricTaggedConsumer.class, config.root().render(ConfigRenderOptions.concise()),1);
        }

        stormConfig.setNumWorkers(numOfTotalWorkers);
        StormTopology topology = buildTopology(topologyId, numOfSpoutTasks, numOfRouterBolts, numOfAlertBolts, numOfPublishTasks, config);

        if(localMode) {
            LOG.info("Submitting as local mode");
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology(topologyId, stormConfig, topology);
            Utils.sleep(Long.MAX_VALUE);
        }else{
            LOG.info("Submitting as cluster mode");
            try {
                StormSubmitter.submitTopologyWithProgressBar(topologyId, stormConfig, topology);
            } catch(Exception ex) {
                LOG.error("fail submitting topology {}", topology, ex);
                throw new IllegalStateException(ex);
            }
        }
    }

    public void run(Config config) {
        String topologyId = config.getString("topology.name");
        run(topologyId,config);
    }

    public void run(String topologyId,Config config) {
        int numOfSpoutTasks = config.getInt(SPOUT_TASK_NUM);
        int numOfRouterBolts = config.getInt(ROUTER_TASK_NUM);
        int numOfAlertBolts = config.getInt(ALERT_TASK_NUM);
        int numOfPublishTasks = config.getInt(PUBLISH_TASK_NUM);
        boolean localMode = config.getBoolean(LOCAL_MODE);
        int numOfTotalWorkers = config.getInt(TOTAL_WORKER_NUM);
        run(topologyId,numOfTotalWorkers, numOfSpoutTasks,numOfRouterBolts,numOfAlertBolts,numOfPublishTasks,config, localMode);
    }

    public StormTopology buildTopology(String topologyId,Config config) {
        int numOfSpoutTasks = config.getInt("topology.numOfSpoutTasks");
        int numOfRouterBolts = config.getInt("topology.numOfRouterBolts");
        int numOfAlertBolts = config.getInt("topology.numOfAlertBolts");
        int numOfPublishTasks = config.getInt("topology.numOfPublishTasks");
        return buildTopology(topologyId,numOfSpoutTasks,numOfRouterBolts,numOfAlertBolts,numOfPublishTasks,config);
    }

    public IMetadataChangeNotifyService getMetadataChangeNotifyService() {
        return metadataChangeNotifyService;
    }
}
