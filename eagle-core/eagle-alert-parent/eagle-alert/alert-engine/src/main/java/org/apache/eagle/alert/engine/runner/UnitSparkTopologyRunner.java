/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.eagle.alert.engine.runner;

import com.typesafe.config.Config;
import kafka.serializer.StringDecoder;
import org.apache.eagle.alert.coordination.model.AlertBoltSpec;
import org.apache.eagle.alert.coordination.model.PublishSpec;
import org.apache.eagle.alert.coordination.model.RouterSpec;
import org.apache.eagle.alert.coordination.model.SpoutSpec;
import org.apache.eagle.alert.engine.coordinator.IMetadataChangeNotifyService;
import org.apache.eagle.alert.engine.coordinator.MetadataType;
import org.apache.eagle.alert.engine.coordinator.StreamDefinition;
import org.apache.eagle.alert.engine.router.SpecListener;
import org.apache.eagle.alert.engine.spark.broadcast.*;
import org.apache.eagle.alert.engine.spark.function.*;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class UnitSparkTopologyRunner implements SpecListener {

    private static final Logger LOG = LoggerFactory.getLogger(UnitSparkTopologyRunner.class);

    public final static String WINDOW_SECOND = "topology.window";
    public final static int DEFAULT_WINDOW_SECOND = 2;
    public final static String SPARK_EXECUTOR_CORES = "topology.core";
    public final static String SPARK_EXECUTOR_MEMORY = "topology.memory";
    public final static String alertBoltNamePrefix = "alertBolt";
    public final static String alertPublishBoltName = "alertPublishBolt";
    public final static String SPARK_EXECUTOR_INSTANCES = "topology.spark.executor.num"; //no need to set if you open spark.dynamicAllocation.enabled  see https://spark.apache.org/docs/latest/job-scheduling.html#dynamic-resource-allocation
    public final static String LOCAL_MODE = "topology.localMode";
    public final static String ROUTER_TASK_NUM = "topology.numOfRouterBolts";
    public final static String ALERT_TASK_NUM = "topology.numOfAlertBolts";
    public final static String PUBLISH_TASK_NUM = "topology.numOfPublishTasks";
    public final static String CONSUMER_KAFKA_TOPIC = "topology.topics";
    public final static String WINDOW_DURATIONS = "topology.windowDurations";
    public final static String CHECKPOINT_DIRECTORY = "topology.checkpointDirectory";


    private final IMetadataChangeNotifyService metadataChangeNotifyService;
    private String topologyId;
    private final Config config;
    private JavaStreamingContext jssc;
    private SpoutSpec spoutSpec = null;
    private RouterSpec routerSpec = null;
    private AlertBoltSpec alertBoltSpec = null;
    private PublishSpec publishSpec = null;
    private Map<String, StreamDefinition> sds = null;

    public UnitSparkTopologyRunner(IMetadataChangeNotifyService metadataChangeNotifyService, Config config) {

        this.topologyId = config.getString("topology.name");
        this.config = config;

        long window = config.hasPath(WINDOW_SECOND) ? config.getLong(WINDOW_SECOND) : DEFAULT_WINDOW_SECOND;
        SparkConf sparkConf = new SparkConf();
        sparkConf.setAppName(topologyId);
        boolean localMode = config.getBoolean(LOCAL_MODE);
        if (localMode) {
            LOG.info("Submitting as local mode");
            sparkConf.setMaster("local[*]");
        }
        String sparkExecutorCores = config.getString(SPARK_EXECUTOR_CORES);
        String sparkExecutorMemory = config.getString(SPARK_EXECUTOR_MEMORY);
       // String checkpointDir = config.getString(CHECKPOINT_DIRECTORY);
        sparkConf.set("spark.executor.cores", sparkExecutorCores);
        sparkConf.set("spark.executor.memory", sparkExecutorMemory);

        this.jssc = new JavaStreamingContext(sparkConf, Durations.seconds(window));
       // this.jssc.checkpoint(checkpointDir);
        this.metadataChangeNotifyService = metadataChangeNotifyService;
        this.metadataChangeNotifyService.registerListener(this);
        this.metadataChangeNotifyService.init(config, MetadataType.ALL);
    }

    public void run() {

        buildTopology(jssc, config);
        jssc.start();
        jssc.awaitTermination();
    }


    private void buildTopology(JavaStreamingContext jssc, Config config) {

        String zkQuorum = config.getString("spout.kafkaBrokerZkQuorum");
        Map<String, String> kafkaParams = new HashMap<>();
        kafkaParams.put("metadata.broker.list", zkQuorum);
        kafkaParams.put("auto.offset.reset", "largest");


        String topic = config.getString(CONSUMER_KAFKA_TOPIC);
        Set<String> topics = new HashSet<String>(Arrays.asList(topic));

        int windowDurations = config.getInt(WINDOW_DURATIONS);
        int numOfRouter = config.getInt(ROUTER_TASK_NUM);
        int numOfAlertBolts = config.getInt(ALERT_TASK_NUM);
        int numOfPublishTasks = config.getInt(PUBLISH_TASK_NUM);

        while (spoutSpec == null || sds == null || routerSpec == null || alertBoltSpec == null || publishSpec == null) {
            System.out.println("wait to load meta");
        }

        JavaPairDStream<String, String> messages = KafkaUtils.createDirectStream(jssc, String.class, String.class, StringDecoder.class, StringDecoder.class, kafkaParams, topics);

        messages.window(Durations.seconds(windowDurations), Durations.seconds(windowDurations))
                .flatMapToPair(new CorrelationSpoutSparkFunction(numOfRouter, topic, spoutSpec, sds))
                .transformToPair(new ChangePartitionTo(numOfRouter))
                .mapPartitionsToPair(new StreamRouteBoltFunction(routerSpec, sds, "streamBolt"))
                .transformToPair(new ChangePartitionTo(numOfAlertBolts)).mapPartitionsToPair(new AlertBoltFunction(alertBoltNamePrefix, alertBoltSpec, sds, numOfAlertBolts))
                .repartition(numOfPublishTasks).foreachRDD(new Publisher(publishSpec, sds, alertPublishBoltName));


    }

    @Override
    public void onSpecChange(SpoutSpec spec, RouterSpec routerSpec, AlertBoltSpec alertBoltSpec, PublishSpec publishSpec, Map<String, StreamDefinition> sds) {
        this.sds = StreamDefinitionData.getInstance(jssc.sparkContext(), sds).value();
        this.routerSpec = RouterSpecData.getInstance(jssc.sparkContext(), routerSpec).value();
        this.spoutSpec = SpoutSpecData.getInstance(jssc.sparkContext(), spec).value();
        this.alertBoltSpec = AlertBoltSpecData.getInstance(jssc.sparkContext(), alertBoltSpec).value();
        this.publishSpec = PublishSpecData.getInstance(jssc.sparkContext(), publishSpec).value();
    }
}
