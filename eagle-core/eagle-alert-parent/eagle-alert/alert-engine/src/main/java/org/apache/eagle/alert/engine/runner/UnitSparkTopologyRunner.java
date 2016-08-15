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

import static org.apache.eagle.alert.engine.utils.SpecUtils.getTopicsByConfig;

import org.apache.eagle.alert.coordination.model.AlertBoltSpec;
import org.apache.eagle.alert.coordination.model.PublishSpec;
import org.apache.eagle.alert.coordination.model.RouterSpec;
import org.apache.eagle.alert.coordination.model.SpoutSpec;
import org.apache.eagle.alert.engine.coordinator.StreamDefinition;
import org.apache.eagle.alert.engine.spark.function.ChangePartitionTo;
import org.apache.eagle.alert.engine.spark.function.CorrelationSpoutSparkFunction;
import org.apache.eagle.alert.engine.spark.function.FetchSpecAndTopicFunction;
import org.apache.eagle.alert.engine.spark.function.RefreshTopicFunction;
import org.apache.eagle.alert.engine.spark.function.StreamRouteBoltFunction;
import org.apache.eagle.alert.engine.spark.function.AlertBoltFunction;
import org.apache.eagle.alert.engine.spark.function.Publisher;

import com.typesafe.config.Config;
import kafka.common.TopicAndPartition;
import kafka.message.MessageAndMetadata;
import kafka.serializer.StringDecoder;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.EagleKafkaUtils;
import org.apache.spark.streaming.kafka.KafkaCluster;
import org.apache.spark.streaming.kafka.OffsetRange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Predef;
import scala.Tuple2;
import scala.collection.JavaConversions;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

public class UnitSparkTopologyRunner implements Serializable {

    private static final Logger LOG = LoggerFactory.getLogger(UnitSparkTopologyRunner.class);
    //kafka config
    private KafkaCluster kafkaCluster = null;
    private Map<String, String> kafkaParams = new HashMap<String, String>();
    private Map<TopicAndPartition, Long> fromOffsets = new HashMap<TopicAndPartition, Long>();
    private final AtomicReference<OffsetRange[]> offsetRanges = new AtomicReference<OffsetRange[]>();
    private final AtomicReference<Map<String, StreamDefinition>> sdsRef = new AtomicReference<Map<String, StreamDefinition>>();
    private final AtomicReference<SpoutSpec> spoutSpecRef = new AtomicReference<SpoutSpec>();
    private final AtomicReference<RouterSpec> routerSpecRef = new AtomicReference<RouterSpec>();
    private final AtomicReference<AlertBoltSpec> alertBoltSpecRef = new AtomicReference<AlertBoltSpec>();
    private final AtomicReference<PublishSpec> publishSpecRef = new AtomicReference<PublishSpec>();
    private final AtomicReference<Set<String>> topicsRef = new AtomicReference<Set<String>>();
    private String groupId;
    //Zookeeper server string: host1:port1[,host2:port2,...]
    private String zkServers = null;
    //spark config
    private static final String BATCH_DURATION = "topology.batchDuration";
    private static final int DEFAULT_BATCH_DURATION_SECOND = 2;
    private static final String SPARK_EXECUTOR_CORES = "topology.core";
    private static final String SPARK_EXECUTOR_MEMORY = "topology.memory";
    private static final String alertBoltNamePrefix = "alertBolt";
    private static final String alertPublishBoltName = "alertPublishBolt";
    private static final String LOCAL_MODE = "topology.localMode";
    private static final String ROUTER_TASK_NUM = "topology.numOfRouterBolts";
    private static final String ALERT_TASK_NUM = "topology.numOfAlertBolts";
    private static final String PUBLISH_TASK_NUM = "topology.numOfPublishTasks";
    private static final String SLIDE_DURATION_SECOND = "topology.slideDurations";
    private static final String WINDOW_DURATIONS_SECOND = "topology.windowDurations";
    private static final String TOPOLOGY_MASTER = "topology.master";
    private SparkConf sparkConf;

    private final Config config;

    public UnitSparkTopologyRunner(Config config) {

        prepareKafkaConfig(config);
        prepareSparkConfig(config);

        this.config = config;
        this.zkServers = config.getString("zkConfig.zkQuorum");

    }

    public void run() throws InterruptedException {
        long batchDuration = config.hasPath(BATCH_DURATION) ? config.getLong(BATCH_DURATION) : DEFAULT_BATCH_DURATION_SECOND;
        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(batchDuration));
        buildTopology(jssc, config);
        LOG.info("Starting Spark Streaming");
        jssc.start();
        LOG.info("Spark Streaming is running");
        jssc.awaitTermination();
    }

    private void buildTopology(JavaStreamingContext jssc, Config config) {

        Set<String> topics = getTopicsByConfig(config);
        EagleKafkaUtils.fillInLatestOffsets(topics,
                this.fromOffsets,
                this.groupId,
                this.kafkaCluster,
                this.zkServers);

        int windowDurations = config.getInt(WINDOW_DURATIONS_SECOND);
        int slideDurations = config.getInt(SLIDE_DURATION_SECOND);
        int numOfRouter = config.getInt(ROUTER_TASK_NUM);
        int numOfAlertBolts = config.getInt(ALERT_TASK_NUM);
        int numOfPublishTasks = config.getInt(PUBLISH_TASK_NUM);

        @SuppressWarnings("unchecked")
        Class<MessageAndMetadata<String, String>> streamClass =
                (Class<MessageAndMetadata<String, String>>) (Class<?>) MessageAndMetadata.class;

        JavaInputDStream<MessageAndMetadata<String, String>> messages = EagleKafkaUtils.createDirectStream(jssc,
                String.class,
                String.class,
                StringDecoder.class,
                StringDecoder.class,
                streamClass,
                kafkaParams,
                this.fromOffsets,
                new RefreshTopicFunction(this.topicsRef, this.groupId, this.kafkaCluster, this.zkServers),
                message -> message);

        JavaPairDStream<String, String> pairDStream = messages
                .transform(new FetchSpecAndTopicFunction(offsetRanges,
                        spoutSpecRef,
                        sdsRef,
                        alertBoltSpecRef,
                        publishSpecRef,
                        routerSpecRef,
                        topicsRef,
                        config))
                .mapToPair(km -> new Tuple2<>(km.topic(), km.message()));

        pairDStream
                .window(Durations.seconds(windowDurations), Durations.seconds(slideDurations))
                .flatMapToPair(new CorrelationSpoutSparkFunction(numOfRouter, spoutSpecRef, sdsRef))
                .transformToPair(new ChangePartitionTo(numOfRouter))
                .mapPartitionsToPair(new StreamRouteBoltFunction("streamBolt", routerSpecRef, sdsRef))
                .transformToPair(new ChangePartitionTo(numOfAlertBolts))
                .mapPartitionsToPair(new AlertBoltFunction(alertBoltNamePrefix, sdsRef, alertBoltSpecRef, numOfAlertBolts))
                .repartition(numOfPublishTasks)
                .foreachRDD(new Publisher(publishSpecRef, sdsRef, alertPublishBoltName, kafkaCluster, groupId, offsetRanges));
    }

    private void prepareKafkaConfig(Config config) {
        String inputBroker = config.getString("spout.kafkaBrokerZkQuorum");
        this.kafkaParams.put("metadata.broker.list", inputBroker);
        this.groupId = "eagle" + new Random(10).nextInt();
        this.kafkaParams.put("group.id", this.groupId);
        this.kafkaParams.put("auto.offset.reset", "largest");
        // Newer version of metadata.broker.list:
        this.kafkaParams.put("bootstrap.servers", inputBroker);

        scala.collection.mutable.Map<String, String> mutableKafkaParam = JavaConversions
                .mapAsScalaMap(kafkaParams);
        scala.collection.immutable.Map<String, String> immutableKafkaParam = mutableKafkaParam
                .toMap(new Predef.$less$colon$less<Tuple2<String, String>, Tuple2<String, String>>() {
                    public Tuple2<String, String> apply(
                            Tuple2<String, String> v1) {
                        return v1;
                    }
                });
        this.kafkaCluster = new KafkaCluster(immutableKafkaParam);
    }

    private void prepareSparkConfig(Config config) {
        SparkConf sparkConf = new SparkConf();
        sparkConf.setAppName(config.getString("topology.name"));
        boolean localMode = config.getBoolean(LOCAL_MODE);
        if (localMode) {
            LOG.info("Submitting as local mode");
            sparkConf.setMaster("local[*]");
        } else {
            sparkConf.setMaster(config.getString(TOPOLOGY_MASTER));
        }
        String sparkExecutorCores = config.getString(SPARK_EXECUTOR_CORES);
        String sparkExecutorMemory = config.getString(SPARK_EXECUTOR_MEMORY);
        sparkConf.set("spark.executor.cores", sparkExecutorCores);
        sparkConf.set("spark.executor.memory", sparkExecutorMemory);
        sparkConf.set("spark.streaming.dynamicAllocation.enable", "true");
        this.sparkConf = sparkConf;
    }
}
