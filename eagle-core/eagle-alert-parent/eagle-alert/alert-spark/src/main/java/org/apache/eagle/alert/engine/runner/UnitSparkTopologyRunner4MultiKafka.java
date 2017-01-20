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
import org.apache.commons.collections.CollectionUtils;
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
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.EagleKafkaUtils4MultiKafka;
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

/**
 * spark topology runner for multikafka.
 */
public class UnitSparkTopologyRunner4MultiKafka implements Serializable {

    private static final long serialVersionUID = 381513979960046346L;

    private static final Logger LOG = LoggerFactory.getLogger(UnitSparkTopologyRunner4MultiKafka.class);
    //kafka config

    //common config
    private final AtomicReference<Map<String, StreamDefinition>> sdsRef = new AtomicReference<>();
    private final AtomicReference<SpoutSpec> spoutSpecRef = new AtomicReference<>();
    private final AtomicReference<AlertBoltSpec> alertBoltSpecRef = new AtomicReference<>();

    private final AtomicReference<RouterSpec> routerSpecRef = new AtomicReference<>();
    private final AtomicReference<PublishSpec> publishSpecRef = new AtomicReference<>();
    public static String groupId;
    //Zookeeper server string: host1:port1[,host2:port2,...]
    private String zkServers = null;
    //spark config
    private static final String BATCH_DURATION = "topology.batchDuration";
    private static final int DEFAULT_BATCH_DURATION_SECOND = 2;
    private static final String SPARK_EXECUTOR_CORES = "topology.core";
    private static final String SPARK_EXECUTOR_MEMORY = "topology.memory";
    private static final String alertPublishBoltName = "alertPublishBolt";
    private static final String LOCAL_MODE = "topology.localMode";
    private static final String ROUTER_TASK_NUM = "topology.numOfRouterBolts";
    private static final String ALERT_TASK_NUM = "topology.numOfAlertBolts";
    private static final String PUBLISH_TASK_NUM = "topology.numOfPublishTasks";
    private static final String SLIDE_DURATION_SECOND = "topology.slideDurations";
    private static final String WINDOW_DURATIONS_SECOND = "topology.windowDurations";
    private static final String TOPOLOGY_MASTER = "topology.master";
    private static final String DRIVER_MEMORY = "topology.driverMemory";
    private static final String DRIVER_CORES = "topology.driverCores";
    private static final String DEPLOY_MODE = "topology.deployMode";
    private static final String CHECKPOINT_PATH = "topology.checkpointPath";


    private final AtomicReference<Map<KafkaClusterInfo, Set<String>>> clusterInfoRef = new AtomicReference<>();
    private Map<KafkaClusterInfo, Map<TopicAndPartition, Long>> fromOffsetsClusterMapRef = new HashMap<>();
    private final AtomicReference<Map<KafkaClusterInfo, OffsetRange[]>> offsetRangesClusterMapRef = new AtomicReference<>();
    public static Class<MessageAndMetadata<String, String>> streamClass = (Class<MessageAndMetadata<String, String>>) (Class<?>) MessageAndMetadata.class;

    private SparkConf sparkConf;

    private final Config config;


    public UnitSparkTopologyRunner4MultiKafka(Config config) {

        prepareKafkaConfig(config);
        prepareSparkConfig(config);
        this.config = config;

    }

    public void run() throws InterruptedException {

        final String checkpointDirectory = config.getString(CHECKPOINT_PATH);
        JavaStreamingContext jssc;
        if (!StringUtils.isEmpty(checkpointDirectory)) {
            Function0<JavaStreamingContext> createContextFunc = (Function0<JavaStreamingContext>) () -> buildTopology(config, checkpointDirectory);
            jssc = JavaStreamingContext.getOrCreate(checkpointDirectory, createContextFunc);
        } else {
            jssc = buildTopology(config, checkpointDirectory);
        }

        LOG.info("Starting Spark Streaming");
        jssc.start();
        LOG.info("Spark Streaming is running");
        jssc.awaitTermination();
    }

    private void prepareKafkaConfig(Config config) {
        this.groupId = config.getString("topology.groupId");
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
        String driverMemory = config.getString(DRIVER_MEMORY);
        String driverCore = config.getString(DRIVER_CORES);
        String deployMode = config.getString(DEPLOY_MODE);
        sparkConf.set("spark.executor.cores", sparkExecutorCores);
        sparkConf.set("spark.executor.memory", sparkExecutorMemory);
        sparkConf.set("spark.driver.memory", driverMemory);
        sparkConf.set("spark.driver.cores", driverCore);
        sparkConf.set("spark.submit.deployMode", deployMode);
        sparkConf.set("spark.streaming.dynamicAllocation.enable", "true");

        this.sparkConf = sparkConf;
    }

    private JavaStreamingContext buildTopology(Config config, String checkpointDirectory) {
        // 1. get kafka topic info from rest client
        Map<String, Map<String, String>> kafkaInfos = getAllTopicsInfoByConfig(config);
        clusterInfoRef.set(getKafkaClustersByKafkaInfo(kafkaInfos, new HashMap<>()));
        LOG.info("clusterInfo : " + clusterInfoRef.get().toString());
        // 2. get offset for each kafka cluster
        EagleKafkaUtils4MultiKafka.fillInLatestOffsetsByCluster(clusterInfoRef.get(), fromOffsetsClusterMapRef, groupId);

        int windowDurations = config.getInt(WINDOW_DURATIONS_SECOND);
        int slideDurations = config.getInt(SLIDE_DURATION_SECOND);
        int numOfRouter = config.getInt(ROUTER_TASK_NUM);
        int numOfAlertBolts = config.getInt(ALERT_TASK_NUM);
        int numOfPublishTasks = config.getInt(PUBLISH_TASK_NUM);
        long batchDuration = config.hasPath(BATCH_DURATION) ? config.getLong(BATCH_DURATION) : DEFAULT_BATCH_DURATION_SECOND;

        @SuppressWarnings("unchecked")
        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(batchDuration));
        if (!StringUtils.isEmpty(checkpointDirectory)) {
            jssc.checkpoint(checkpointDirectory);
        }
        // 3. get all kafka Dstream with refresh topic function
        List<JavaInputDStream<MessageAndMetadata<String, String>>> inputDStreams = buldInputDstreamList(clusterInfoRef, fromOffsetsClusterMapRef, jssc, offsetRangesClusterMapRef);
        // 4. union all kafka Dstream
        JavaInputDStream<MessageAndMetadata<String, String>> inputDStream = inputDStreams.get(0);
        for (int i = 1; i < inputDStreams.size(); i++) {
            inputDStream.union(inputDStreams.get(i));
        }
        JavaDStream<MessageAndMetadata<String, String>> newInputDstream = inputDStream.transform(new RefreshUnionInputStreamFuntion(clusterInfoRef, config, jssc, offsetRangesClusterMapRef));
        // 5. build topology
        WindowState winstate = new WindowState(jssc);
        RouteState routeState = new RouteState(jssc);
        PolicyState policyState = new PolicyState(jssc);
        PublishState publishState = new PublishState(jssc);
        SiddhiState siddhiState = new SiddhiState(jssc);

        JavaPairDStream<String, String> pairDStream = newInputDstream
            .transform(new ProcessSpecFunction4MultiKafka( // 重新获取topic并设置新的topicRef, offsetRange
                spoutSpecRef,
                sdsRef,
                alertBoltSpecRef,
                publishSpecRef,
                clusterInfoRef,
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


    public static List<JavaInputDStream<MessageAndMetadata<String, String>>> buldInputDstreamList(
        AtomicReference<Map<KafkaClusterInfo, Set<String>>> clusterInfoRef,
        Map<KafkaClusterInfo, Map<TopicAndPartition, Long>> fromOffsetsClusterMapRef,
        JavaStreamingContext jssc,
        AtomicReference<Map<KafkaClusterInfo, OffsetRange[]>> offsetRangesClusterMapRef) {
        List<JavaInputDStream<MessageAndMetadata<String, String>>> inputDStreams = Lists.newArrayList();
        Map<KafkaClusterInfo, Set<String>> clusterInfoMap = clusterInfoRef.get();
        for (KafkaClusterInfo kafkaClusterInfo : clusterInfoMap.keySet()) {
            Map<String, String> kafkaParams = buildKafkaParam(kafkaClusterInfo);
            Map<TopicAndPartition, Long> fromOffsets = fromOffsetsClusterMapRef.get(kafkaClusterInfo);
            AtomicReference<HashSet<String>> topicsRef = new AtomicReference<>();
            AtomicReference<OffsetRange[]> offsetRangesRef = new AtomicReference<>();
            JavaInputDStream<MessageAndMetadata<String, String>> messages = EagleKafkaUtils4MultiKafka.createDirectStream(jssc,
                String.class,
                String.class,
                StringDecoder.class,
                StringDecoder.class,
                streamClass,
                kafkaParams,
                fromOffsets,
                new RefreshTopicFunction4MultiKafka(clusterInfoMap.get(kafkaClusterInfo),
                    groupId,
                    kafkaClusterInfo.getKafkaCluster(),
                    kafkaClusterInfo.getZkQuorum()
                ),
                new GetOffsetRangeFunction(offsetRangesClusterMapRef, clusterInfoRef),
                message -> message);
            inputDStreams.add(messages);
        }
        return inputDStreams;
    }

    /**
     * build kafkaCluster with kafkainfo.
     *
     * @param kafkaInfos
     */
    public static Map<KafkaClusterInfo, Set<String>> getKafkaClustersByKafkaInfo(Map<String, Map<String, String>> kafkaInfos, Map<KafkaClusterInfo, Set<String>> cachedClusters) {
        Map<KafkaClusterInfo, Set<String>> clusters = Maps.newHashMap();
        for (String topic : kafkaInfos.keySet()) {
            Map<String, String> kafkaProperties = kafkaInfos.get(topic);
            String kafkaBrokerZkQuorum = kafkaProperties.get("spout.kafkaBrokerZkQuorum");
            String kafkaBrokerPathQuorum = kafkaProperties.get("spout.kafkaBrokerZkBasePath");
            if (StringUtils.isEmpty(kafkaBrokerZkQuorum) || StringUtils.isEmpty(kafkaBrokerPathQuorum)) {
                continue;
            }
            final KafkaClusterInfo clusterInfo = new KafkaClusterInfo(topic, kafkaBrokerZkQuorum);
            Set<String> clusterTopics = clusters.get(clusterInfo);
            if (CollectionUtils.isNotEmpty(clusterTopics)) {
                clusterTopics.add(topic);
            } else {
                Optional<KafkaClusterInfo> cachedCluster = cachedClusters.keySet().stream().filter(item -> item.equals(clusterInfo)).findFirst();
                if (cachedCluster.isPresent()) {
                    clusters.put(cachedCluster.get(), Sets.newHashSet(topic));
                } else {
                    String brokerList = listKafkaBrokersByZk(kafkaBrokerZkQuorum, kafkaBrokerPathQuorum);
                    LOG.info("brokerlist :" + brokerList);
                    clusterInfo.setBrokerList(brokerList);
                    Map<String, String> kafkaParam = buildKafkaParam(clusterInfo);
                    KafkaCluster cluster = new KafkaCluster(JavaConverters.mapAsScalaMapConverter(kafkaParam).asScala().toMap(
                        Predef.<Tuple2<String, String>>conforms()
                    ));
                    clusterInfo.setKafkaCluster(cluster);
                    clusters.put(clusterInfo, Sets.newHashSet(topic));
                }

            }
        }
        return clusters;
    }

    /**
     * get kafka topic info.
     *
     * @param config
     * @return
     */
    private Map<String, Map<String, String>> getAllTopicsInfoByConfig(Config config) {
        Map<String, Map<String, String>> dataSourceProperties = new HashMap<>();
        List<Kafka2TupleMetadata> kafka2TupleMetadataList = new ArrayList<>();
        try {
            LOG.info("get topics By config");
            IMetadataServiceClient client = new MetadataServiceClientImpl(config);
            kafka2TupleMetadataList = client.listDataSources();
        } catch (Exception e) {
            LOG.error("getTopicsByConfig error :" + e.getMessage(), e);
        }
        for (Kafka2TupleMetadata ds : kafka2TupleMetadataList) {
            // ds.getProperties().put("spout.kafkaBrokerZkQuorum", "localhost:2181");
            dataSourceProperties.put(ds.getTopic(), ds.getProperties());
        }
        return dataSourceProperties;
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
        kafkaParam.put("auto.offset.reset", "largest");
        kafkaParam.put("metadata.broker.list", kafkaClusterInfo.getBrokerList());
        // Newer version of metadata.broker.list:
        kafkaParam.put("bootstrap.servers", kafkaClusterInfo.getBrokerList());
        kafkaParam.put("spout.kafkaBrokerZkQuorum", kafkaClusterInfo.getZkQuorum());
        return kafkaParam;
    }
}
