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


import org.apache.commons.lang3.StringUtils;
import org.apache.eagle.alert.coordination.model.*;
import org.apache.eagle.alert.engine.coordinator.*;
import org.apache.eagle.alert.engine.spark.function.*;
import org.apache.eagle.alert.engine.spark.model.*;

import kafka.common.TopicAndPartition;
import com.typesafe.config.Config;
import kafka.message.MessageAndMetadata;
import kafka.serializer.StringDecoder;
import org.apache.eagle.alert.engine.spark.partition.StreamRoutePartitioner;
import org.apache.eagle.alert.service.IMetadataServiceClient;
import org.apache.eagle.alert.service.MetadataServiceClientImpl;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function0;
import org.apache.spark.api.java.function.PairFunction;
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
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.eagle.alert.engine.utils.Constants.ALERT_TASK_NUM;
import static org.apache.eagle.alert.engine.utils.Constants.AUTO_OFFSET_RESET;
import static org.apache.eagle.alert.engine.utils.Constants.BATCH_DURATION;
import static org.apache.eagle.alert.engine.utils.Constants.CHECKPOINT_PATH;
import static org.apache.eagle.alert.engine.utils.Constants.DEFAULT_BATCH_DURATION_SECOND;
import static org.apache.eagle.alert.engine.utils.Constants.PUBLISH_TASK_NUM;
import static org.apache.eagle.alert.engine.utils.Constants.ROUTER_TASK_NUM;
import static org.apache.eagle.alert.engine.utils.Constants.SLIDE_DURATION_SECOND;
import static org.apache.eagle.alert.engine.utils.Constants.SPOUT_KAFKABROKERZKQUORUM;
import static org.apache.eagle.alert.engine.utils.Constants.TOPOLOGY_GROUPID;
import static org.apache.eagle.alert.engine.utils.Constants.WINDOW_DURATIONS_SECOND;
import static org.apache.eagle.alert.engine.utils.Constants.ZKCONFIG_ZKQUORUM;
import static org.apache.eagle.alert.engine.utils.Constants.alertPublishBoltName;

public class UnitSparkTopologyRunner implements Serializable {

    private static final long serialVersionUID = 381513979960046346L;

    private static final Logger LOG = LoggerFactory.getLogger(UnitSparkTopologyRunner.class);
    //kafka config
    private KafkaCluster kafkaCluster = null;
    private Map<String, String> kafkaParams = new HashMap<>();
    private Map<TopicAndPartition, Long> fromOffsets = new HashMap<>();
    private final AtomicReference<OffsetRange[]> offsetRanges = new AtomicReference<>();
    //common config
    private final AtomicReference<Map<String, StreamDefinition>> sdsRef = new AtomicReference<>();
    private final AtomicReference<SpoutSpec> spoutSpecRef = new AtomicReference<>();
    private final AtomicReference<AlertBoltSpec> alertBoltSpecRef = new AtomicReference<>();
    private final AtomicReference<HashSet<String>> topicsRef = new AtomicReference<>();
    private final AtomicReference<RouterSpec> routerSpecRef = new AtomicReference<>();
    private final AtomicReference<PublishSpec> publishSpecRef = new AtomicReference<>();
    private String groupId;
    //Zookeeper server string: host1:port1[,host2:port2,...]
    private String zkServers = null;
    private SparkConf sparkConf = new SparkConf();

    private final Config config;


    public UnitSparkTopologyRunner(Config config) {

        prepareKafkaConfig(config);
        this.config = config;
        this.zkServers = config.getString(ZKCONFIG_ZKQUORUM);
    }

    public UnitSparkTopologyRunner(Config config, SparkConf sparkConf) {
        this(config);
        this.sparkConf = sparkConf;
    }

    public void run() throws InterruptedException {

        JavaStreamingContext jssc = this.buildTopology();
        LOG.info("Starting Spark Streaming");
        jssc.start();
        LOG.info("Spark Streaming is running");
        jssc.awaitTermination();
    }

    public JavaStreamingContext buildTopology() {

        final String checkpointDirectory = config.getString(CHECKPOINT_PATH);
        JavaStreamingContext jssc;
        if (!StringUtils.isEmpty(checkpointDirectory)) {
            Function0<JavaStreamingContext> createContextFunc = (Function0<JavaStreamingContext>) () -> buildAllTopology(config, checkpointDirectory);
            jssc = JavaStreamingContext.getOrCreate(checkpointDirectory, createContextFunc);
        } else {
            jssc = buildAllTopology(config, checkpointDirectory);
        }

        return jssc;
    }

    private JavaStreamingContext buildAllTopology(Config config, String checkpointDirectory) {

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
        long batchDuration = config.hasPath(BATCH_DURATION) ? config.getLong(BATCH_DURATION) : DEFAULT_BATCH_DURATION_SECOND;
        @SuppressWarnings("unchecked")
        Class<MessageAndMetadata<String, String>> streamClass =
            (Class<MessageAndMetadata<String, String>>) (Class<?>) MessageAndMetadata.class;
        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(batchDuration));
        if (!StringUtils.isEmpty(checkpointDirectory)) {
            jssc.checkpoint(checkpointDirectory);
        }
        JavaInputDStream<MessageAndMetadata<String, String>> messages = EagleKafkaUtils.createDirectStream(jssc,
            String.class,
            String.class,
            StringDecoder.class,
            StringDecoder.class,
            streamClass,
            kafkaParams,
            this.fromOffsets,
            new RefreshTopicFunction(this.topicsRef, this.groupId, this.kafkaCluster, this.zkServers), message -> message);

        WindowState winstate = new WindowState();
        RouteState routeState = new RouteState();
        PolicyState policyState = new PolicyState();
        PublishState publishState = new PublishState();
        SiddhiState siddhiState = new SiddhiState();


        JavaPairDStream<String, String> pairDStream = messages
            .transform(new ProcessSpecFunction(offsetRanges,
                spoutSpecRef,
                sdsRef,
                alertBoltSpecRef,
                publishSpecRef,
                topicsRef,
                routerSpecRef,
                config,
                winstate,
                routeState,
                policyState,
                publishState,
                siddhiState,
                numOfAlertBolts))
            .mapToPair((PairFunction<MessageAndMetadata<String, String>, String, String>) km -> new Tuple2<String, String>(km.topic(), km.message()));

        pairDStream
            .window(Durations.seconds(windowDurations), Durations.seconds(slideDurations))
            .flatMapToPair(new CorrelationSpoutSparkFunction(numOfRouter, spoutSpecRef, sdsRef))
            .groupByKey(new StreamRoutePartitioner(numOfRouter))
            .mapPartitionsToPair(new StreamRouteBoltFunction("streamBolt", sdsRef, routerSpecRef, winstate, routeState))
            .groupByKey(new StreamRoutePartitioner(numOfAlertBolts))
            .mapPartitionsToPair(new AlertBoltFunction(sdsRef, alertBoltSpecRef, policyState, siddhiState, publishState))
            .groupByKey(numOfPublishTasks)
            .foreachRDD(new Publisher(alertPublishBoltName, kafkaCluster, groupId, offsetRanges, publishState, publishSpecRef, config));
        return jssc;
    }

    private void prepareKafkaConfig(Config config) {
        String inputBroker = config.getString(SPOUT_KAFKABROKERZKQUORUM);
        this.kafkaParams.put("metadata.broker.list", inputBroker);
        this.groupId = config.getString(TOPOLOGY_GROUPID);
        this.kafkaParams.put("group.id", this.groupId);
        String reset = config.hasPath(AUTO_OFFSET_RESET) ? config.getString(AUTO_OFFSET_RESET) : "largest";
        this.kafkaParams.put("auto.offset.reset", reset);
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


    private Set<String> getTopicsByConfig(Config config) {
        Set<String> topics = new HashSet<>();
        List<Kafka2TupleMetadata> kafka2TupleMetadata = new ArrayList<>();
        try {
            IMetadataServiceClient client = new MetadataServiceClientImpl(config);
            kafka2TupleMetadata = client.listDataSources();
        } catch (Exception e) {
            LOG.error("getTopicsByConfig error :" + e.getMessage(), e);
        }

        for (Kafka2TupleMetadata eachKafka2TupleMetadata : kafka2TupleMetadata) {
            topics.add(eachKafka2TupleMetadata.getTopic());
        }
        return topics;
    }
}
