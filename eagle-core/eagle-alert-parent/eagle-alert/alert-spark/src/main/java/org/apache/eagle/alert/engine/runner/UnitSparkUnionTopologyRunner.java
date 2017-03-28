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


import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.typesafe.config.Config;
import kafka.common.TopicAndPartition;
import kafka.message.MessageAndMetadata;
import kafka.serializer.StringDecoder;
import org.apache.commons.lang3.StringUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryNTimes;
import org.apache.eagle.alert.coordination.model.*;
import org.apache.eagle.alert.engine.coordinator.StreamDefinition;
import org.apache.eagle.alert.engine.spark.function.*;
import org.apache.eagle.alert.engine.spark.model.*;
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
import org.json.simple.JSONValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Predef;
import scala.Tuple2;
import scala.collection.JavaConverters;

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
import static org.apache.eagle.alert.engine.utils.Constants.TOPOLOGY_GROUPID;
import static org.apache.eagle.alert.engine.utils.Constants.WINDOW_DURATIONS_SECOND;
import static org.apache.eagle.alert.engine.utils.Constants.alertPublishBoltName;
/**
 * spark topology runner for multikafka.
 */
public class UnitSparkUnionTopologyRunner implements Serializable {

    private static final long serialVersionUID = 391513979960046346L;

    private static final Logger LOG = LoggerFactory.getLogger(UnitSparkUnionTopologyRunner.class);


    //kafka config

    //common config
    private final AtomicReference<Map<String, StreamDefinition>> sdsRef = new AtomicReference<>();
    private final AtomicReference<SpoutSpec> spoutSpecRef = new AtomicReference<>();
    private final AtomicReference<AlertBoltSpec> alertBoltSpecRef = new AtomicReference<>();

    private final AtomicReference<RouterSpec> routerSpecRef = new AtomicReference<>();
    private final AtomicReference<PublishSpec> publishSpecRef = new AtomicReference<>();
    public static String groupId;

    private final AtomicReference<Map<KafkaClusterInfo, OffsetRange[]>> offsetRangesClusterMapRef = new AtomicReference<>();
    private static Class<MessageAndMetadata<String, String>> streamClass = (Class<MessageAndMetadata<String, String>>) (Class<?>) MessageAndMetadata.class;


    private static Config config;


    public UnitSparkUnionTopologyRunner(Config config) {

        prepareKafkaConfig(config);
        this.config = config;

    }

    public void run() throws InterruptedException {
        JavaStreamingContext jssc = this.buildTopology();
        LOG.info("Starting Spark Streaming");
        jssc.start();
        LOG.info("Spark Streaming is running");
        jssc.awaitTermination();
    }

    private void prepareKafkaConfig(Config config) {
        this.groupId = config.getString(TOPOLOGY_GROUPID);
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
        int windowDurations = config.getInt(WINDOW_DURATIONS_SECOND);
        int slideDurations = config.getInt(SLIDE_DURATION_SECOND);
        int numOfRouter = config.getInt(ROUTER_TASK_NUM);
        int numOfAlertBolts = config.getInt(ALERT_TASK_NUM);
        int numOfPublishTasks = config.getInt(PUBLISH_TASK_NUM);
        long batchDuration = config.hasPath(BATCH_DURATION) ? config.getLong(BATCH_DURATION) : DEFAULT_BATCH_DURATION_SECOND;
        SparkConf sparkConf = new SparkConf();
        @SuppressWarnings("unchecked")
        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(batchDuration));
        if (!StringUtils.isEmpty(checkpointDirectory)) {
            jssc.checkpoint(checkpointDirectory);
        }
        // 1. build union dstream
        JavaInputDStream<MessageAndMetadata<String, String>> inputDStream = EagleKafkaUtils.createUnionDirectStream(
            jssc,
            String.class,
            String.class,
            StringDecoder.class,
            StringDecoder.class,
            streamClass,
            new ArrayList<KafkaClusterInfo>(),
            new RefreshClusterAndTopicFunction(config),
            new GetOffsetRangeFunction(offsetRangesClusterMapRef),
            message -> message
        );

        WindowState winstate = new WindowState();
        RouteState routeState = new RouteState();
        PolicyState policyState = new PolicyState();
        PublishState publishState = new PublishState();
        SiddhiState siddhiState = new SiddhiState();

        JavaPairDStream<String, String> pairDStream = inputDStream
            .transform(new ProcessSpecFunction4MultiKafka(
                spoutSpecRef,
                sdsRef,
                alertBoltSpecRef,
                publishSpecRef,
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
            .foreachRDD(new Publisher4MultiKafka(alertPublishBoltName, groupId, offsetRangesClusterMapRef, publishState, publishSpecRef, config));
        return jssc;
    }

    public static List<KafkaClusterInfo> getKafkaCLusterInfoByCache(List<KafkaClusterInfo> cachedKafkaClusterInfo) {
        Set<KafkaClusterInfo> resutlSet = Sets.newHashSet();
        List<Kafka2TupleMetadata> kafka2TupleMetadataList = getAllTopicsInfoByConfig(config);
        for (Kafka2TupleMetadata kafka2TupleMetadata : kafka2TupleMetadataList) {
            if ("KAFKA".equals(kafka2TupleMetadata.getType())) {
                String kafkaBrokerZkQuorum = kafka2TupleMetadata.getProperties().get("spout.kafkaBrokerZkQuorum");
                String kafkaBrokerPathQuorum = kafka2TupleMetadata.getProperties().get("spout.kafkaBrokerZkBasePath");
                String topic = kafka2TupleMetadata.getTopic();
                Map<TopicAndPartition, Long> offsets = Maps.newHashMap();
                if (StringUtils.isEmpty(kafkaBrokerZkQuorum) || StringUtils.isEmpty(kafkaBrokerPathQuorum)) {
                    continue;
                }
                KafkaClusterInfo clusterInfo = new KafkaClusterInfo(kafkaBrokerZkQuorum);
                KafkaClusterInfo finalClusterInfo = clusterInfo;
                Optional<KafkaClusterInfo> cachedCluster = cachedKafkaClusterInfo.stream().filter(item -> item.equals(finalClusterInfo)).findFirst();
                if (cachedCluster.isPresent()) {
                    clusterInfo = cachedCluster.get();
                    offsets = clusterInfo.getOffsets();
                    clusterInfo.addTopic(topic);
                } else {
                    Optional<KafkaClusterInfo> clusterInList = resutlSet.stream().filter(item -> item.equals(finalClusterInfo)).findFirst();
                    if (clusterInList.isPresent()) {
                        clusterInfo = clusterInList.get();
                        offsets = clusterInfo.getOffsets();
                        clusterInfo.addTopic(topic);
                    } else {
                        // get kafka broker
                        String brokerList = listKafkaBrokersByZk(kafkaBrokerZkQuorum, kafkaBrokerPathQuorum);
                        LOG.info("brokerlist :" + brokerList);
                        clusterInfo.setBrokerList(brokerList);
                        // build kafka param
                        Map<String, String> kafkaParam = buildKafkaParam(clusterInfo);
                        // build kafkaCluster
                        KafkaCluster cluster = new KafkaCluster(JavaConverters.mapAsScalaMapConverter(kafkaParam).asScala().toMap(
                            Predef.<Tuple2<String, String>>conforms()
                        ));
                        clusterInfo.setKafkaCluster(cluster);
                        clusterInfo.setKafkaParams(kafkaParam);
                        clusterInfo.addTopic(topic);
                    }
                }
                // refresh current offset
                Map<TopicAndPartition, Long> newOffset = EagleKafkaUtils.refreshUnionOffsets(
                    clusterInfo.getTopics(),
                    offsets,
                    groupId,
                    clusterInfo.getKafkaCluster(),
                    clusterInfo.getZkQuorum());
                clusterInfo.setOffsets(newOffset);
                resutlSet.add(clusterInfo);
            }
        }
        return Lists.newArrayList(resutlSet);
    }

    /**
     * get kafka topic info.
     *
     * @param config
     * @return
     */
    private static List<Kafka2TupleMetadata> getAllTopicsInfoByConfig(Config config) {
        List<Kafka2TupleMetadata> kafka2TupleMetadataList = new ArrayList<>();
        try {
            LOG.info("get topics By config");
            IMetadataServiceClient client = new MetadataServiceClientImpl(config);
            kafka2TupleMetadataList = client.listDataSources();
        } catch (Exception e) {
            LOG.error("getTopicsByConfig error :" + e.getMessage(), e);
        }
        return kafka2TupleMetadataList;
    }

    private static String listKafkaBrokersByZk(String kafkaBrokerZkQuorum, String kafkaBrokerZkPath) {
        Set<String> brokerList = Sets.newHashSet();
        CuratorFramework curator = null;
        try {
            curator = CuratorFrameworkFactory.newClient(
                kafkaBrokerZkQuorum,
                1000,
                1000,
                new RetryNTimes(3, 1000)
            );
            curator.start();
            List<String> ids = curator.getChildren().forPath(kafkaBrokerZkPath + "/ids");
            for (String id : ids) {
                Map e = (Map) JSONValue.parse(new String(curator.getData().forPath(kafkaBrokerZkPath + "/ids/" + id), "UTF-8"));
                String host = (String) e.get("host");
                Integer port = Integer.valueOf(((Long) e.get("port")).intValue());
                brokerList.add(host + ":" + port);
            }
        } catch (Exception e) {
            LOG.error("listKafkaBrokersByZk error :" + e.getMessage(), e);
        } finally {
            curator.close();
        }
        return String.join(",", brokerList);
    }

    private static Map<String, String> buildKafkaParam(KafkaClusterInfo kafkaClusterInfo) {
        Map<String, String> kafkaParam = Maps.newHashMap();
        kafkaParam.put("group.id", groupId);
        String reset = config.hasPath(AUTO_OFFSET_RESET) ? config.getString(AUTO_OFFSET_RESET) : "largest";
        kafkaParam.put("auto.offset.reset", reset);
        kafkaParam.put("metadata.broker.list", kafkaClusterInfo.getBrokerList());
        // Newer version of metadata.broker.list:
        kafkaParam.put("bootstrap.servers", kafkaClusterInfo.getBrokerList());
        kafkaParam.put("spout.kafkaBrokerZkQuorum", kafkaClusterInfo.getZkQuorum());
        return kafkaParam;
    }
}
