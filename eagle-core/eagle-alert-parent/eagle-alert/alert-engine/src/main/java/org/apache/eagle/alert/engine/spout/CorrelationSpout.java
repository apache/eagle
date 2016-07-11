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
package org.apache.eagle.alert.engine.spout;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections.CollectionUtils;
import org.apache.eagle.alert.coordination.model.Kafka2TupleMetadata;
import org.apache.eagle.alert.coordination.model.SpoutSpec;
import org.apache.eagle.alert.engine.coordinator.IMetadataChangeNotifyService;
import org.apache.eagle.alert.engine.coordinator.MetadataType;
import org.apache.eagle.alert.engine.coordinator.StreamDefinition;
import org.apache.eagle.alert.engine.router.SpoutSpecListener;
import org.apache.eagle.alert.engine.serialization.PartitionedEventSerializer;
import org.apache.eagle.alert.engine.serialization.SerializationMetadataProvider;
import org.apache.eagle.alert.engine.serialization.Serializers;
import org.apache.eagle.alert.utils.AlertConstants;
import org.apache.eagle.alert.utils.StreamIdConversion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpoutMetric;
import storm.kafka.KafkaSpoutWrapper;
import storm.kafka.SpoutConfig;
import storm.kafka.ZkHosts;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;

import com.typesafe.config.Config;

/**
 * wrap KafkaSpout to provide parallel processing of messages for multiple Kafka topics
 *
 * 1. onNewConfig() is interface for outside to update new metadata. Upon new metadata, this class will calculate if there is any new topic, removed topic or
 *    updated topic
 *
 */
public class CorrelationSpout extends BaseRichSpout implements SpoutSpecListener,SerializationMetadataProvider {
    private static final long serialVersionUID = -5280723341236671580L;
    private static final Logger LOG  = LoggerFactory.getLogger(CorrelationSpout.class);

    public static final String DEFAULT_STORM_KAFKA_TRANSACTION_ZK_ROOT = "/consumers";
    public static final String DEFAULT_STORM_KAFKA_TRANSACTION_ZK_RELATIVE_PATH = "/eagle_consumer";

    // topic to KafkaSpoutWrapper
    private volatile Map<String, KafkaSpoutWrapper> kafkaSpoutList = new HashMap<>();
    private int numOfRouterBolts;

    private SpoutSpec cachedSpoutSpec;

    private transient KafkaSpoutMetric kafkaSpoutMetric;

    @SuppressWarnings("rawtypes")
    private Map conf;
    private TopologyContext context;
    private SpoutOutputCollector collector;
    private final Config config;
    private String topologyId;
    private String spoutName;
    private String routeBoltName;
    @SuppressWarnings("unused")
    private int taskIndex;
    private IMetadataChangeNotifyService changeNotifyService;
    private PartitionedEventSerializer serializer;
    private volatile Map<String, StreamDefinition> sds;

    /**
     * FIXME one single changeNotifyService may have issues as possibly multiple spout tasks will register themselves and initialize service
     * @param config
     * @param topologyId
     * @param changeNotifyService
     * @param numOfRouterBolts
     */
    public CorrelationSpout(Config config, String topologyId, IMetadataChangeNotifyService changeNotifyService, int numOfRouterBolts){
        this(config, topologyId, changeNotifyService, numOfRouterBolts, AlertConstants.DEFAULT_SPOUT_NAME, AlertConstants.DEFAULT_ROUTERBOLT_NAME);
    }
    /**
     *
     * @param config
     * @param topologyId used for distinguishing kafka offset for different topologies
     * @param numOfRouterBolts used for generating streamId and routing
     * @param spoutName used for generating streamId between spout and router bolt
     * @param routerBoltName used for generating streamId between spout and router bolt
     */
    public CorrelationSpout(Config config, String topologyId, IMetadataChangeNotifyService changeNotifyService, int numOfRouterBolts, String spoutName, String routerBoltName){
        this.config = config;
        this.topologyId = topologyId;
        this.changeNotifyService = changeNotifyService;
        this.numOfRouterBolts = numOfRouterBolts;
        this.spoutName = spoutName;
        this.routeBoltName = routerBoltName;
    }

    public String getSpoutName(){
        return spoutName;
    }

    public String getRouteBoltName(){
        return routeBoltName;
    }

    /**
     * the only output field is for StreamEvent
     * @param declarer
     */
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        for (int i = 0; i < numOfRouterBolts; i++) {
            String streamId = StreamIdConversion.generateStreamIdBetween(spoutName, routeBoltName + i);
            declarer.declareStream(streamId, new Fields(AlertConstants.FIELD_0));
            LOG.info("declare stream between spout and streamRouterBolt " + streamId);
        }
    }

    @SuppressWarnings("rawtypes")
    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        if(LOG.isDebugEnabled()) {
            LOG.debug("open method invoked");
        }
        this.conf = conf;
        this.context = context;
        this.collector = collector;
        this.taskIndex = context.getThisTaskIndex();

        // initialize an empty SpoutSpec
        cachedSpoutSpec = new SpoutSpec(topologyId, new HashMap<>(), new HashMap<>(), new HashMap<>());

        changeNotifyService.registerListener(this);
        changeNotifyService.init(config, MetadataType.SPOUT);

        // register KafkaSpout metric
        kafkaSpoutMetric = new KafkaSpoutMetric();
        context.registerMetric("kafkaSpout", kafkaSpoutMetric, 60);

        this.serializer = Serializers.newPartitionedEventSerializer(this);
    }

    @Override
    public void onSpoutSpecChange(SpoutSpec spec, Map<String, StreamDefinition> sds) {
        LOG.info("new metadata is updated " + spec);
        try{
            onReload(spec, sds);
        }catch(Exception ex){
            LOG.error("error applying new SpoutSpec", ex);
        }
    }

    @Override
    public void nextTuple() {
        for (KafkaSpoutWrapper wrapper : kafkaSpoutList.values()) {
            wrapper.nextTuple();
        }
    }

    /**
     * find the correct wrapper to do ack that means msgId should be mapped to
     * wrapper
     *
     * @param msgId
     */
    @Override
    public void ack(Object msgId) {
        // decode and get topic
        KafkaMessageIdWrapper id = (KafkaMessageIdWrapper) msgId;
        KafkaSpoutWrapper spout = kafkaSpoutList.get(id.topic);
        spout.ack(id.id);
    }

    @Override
    public void fail(Object msgId) {
        // decode and get topic
        KafkaMessageIdWrapper id = (KafkaMessageIdWrapper) msgId;
        LOG.error("Failing message {}, with topic {}", msgId, id.topic);
        KafkaSpoutWrapper spout = kafkaSpoutList.get(id.topic);
        spout.fail(id.id);
    }

    @Override
    public void deactivate() {
        System.out.println("deactivate");
        for (KafkaSpoutWrapper wrapper : kafkaSpoutList.values()) {
            wrapper.deactivate();
        }
    }

    @Override
    public void close() {
        System.out.println("close");
        for (KafkaSpoutWrapper wrapper : kafkaSpoutList.values()) {
            wrapper.close();
        }
    }
    
    private List<String> getTopics(SpoutSpec spoutSpec) {
        List<String> meta = new ArrayList<String>();
        for (Kafka2TupleMetadata entry : spoutSpec.getKafka2TupleMetadataMap().values()) {
            meta.add(entry.getTopic());
        }
        return meta;
    }

    @SuppressWarnings("unchecked")
    public void onReload(final SpoutSpec newMeta, Map<String, StreamDefinition> sds) throws Exception {
        // calculate topic create/remove/update
        List<String> topics = getTopics(newMeta);
        List<String> cachedTopcies = getTopics(cachedSpoutSpec);
        Collection<String> newTopics = CollectionUtils.subtract(topics, cachedTopcies);
        Collection<String> removeTopics = CollectionUtils.subtract(cachedTopcies, topics);
        Collection<String> updateTopics = CollectionUtils.intersection(topics, cachedTopcies);

        LOG.info("Topics were added={}, removed={}, modified={}", newTopics, removeTopics, updateTopics);

        // build lookup table for scheme
        Map<String, String> newSchemaName = new HashMap<String, String>();
        for (Kafka2TupleMetadata ds : newMeta.getKafka2TupleMetadataMap().values()) {
            newSchemaName.put(ds.getTopic(), ds.getSchemeCls());
        }

        // copy and swap
        Map<String, KafkaSpoutWrapper> newKafkaSpoutList = new HashMap<>(this.kafkaSpoutList);
        // iterate new topics and then create KafkaSpout
        for(String topic : newTopics){
            KafkaSpoutWrapper wrapper = newKafkaSpoutList.get(topic);
            if (wrapper != null) {
                LOG.warn(MessageFormat.format("try to create new topic {0}, but found in the active spout list, this may indicate some inconsistency", topic));
                continue;
            }
            KafkaSpoutWrapper newWrapper = createKafkaSpout(conf, context, collector, topic, newSchemaName.get(topic), newMeta, sds);
            newKafkaSpoutList.put(topic, newWrapper);
        }
        // iterate remove topics and then close KafkaSpout
        for(String topic : removeTopics){
            KafkaSpoutWrapper wrapper = newKafkaSpoutList.get(topic);
            if (wrapper == null) {
                LOG.warn(MessageFormat.format("try to remove topic {0}, but not found in the active spout list, this may indicate some inconsistency", topic));
                continue;
            }
            removeKafkaSpout(wrapper);
            newKafkaSpoutList.remove(topic);
        }

        // iterate update topic and then update metadata
        for(String topic : updateTopics){
            KafkaSpoutWrapper spoutWrapper = newKafkaSpoutList.get(topic);
            if (spoutWrapper == null) {
                LOG.warn(MessageFormat.format("try to update topic {0}, but not found in the active spout list, this may indicate some inconsistency", topic));
                continue;
            }
            spoutWrapper.update(newMeta, sds);
        }

        // swap
        this.cachedSpoutSpec = newMeta;
        this.kafkaSpoutList = newKafkaSpoutList;
        this.sds = sds;
    }

    /**
     * make this method protected to make sure unit test can work well
     * Q: Where to persist consumer state, i.e. what offset has been consumed for each topic and partition
     * A: stormKafkaTransactionZkPath + "/" + consumerId + "/" + topic + "/" + topologyId + "/" + partitionId
     * Note1: PartitionManager.committedPath for composing zkState path,  _spoutConfig.zkRoot + "/" + _spoutConfig.id + "/" + _partition.getId();
     * consumerId by default is EagleConsumer unless it is specified by "stormKafkaEagleConsumer"
     * Note2: put topologyId as part of zkState because one topic by design can be consumed by multiple topologies so one topology needs to know
     * processed offset for itself
     *
     * TODO: Should avoid use Config.get in deep calling stack, should generate config bean as early as possible
     *
     * @param conf
     * @param context
     * @param collector
     * @param topic
     * @param spoutSpec
     * @return
     */
    @SuppressWarnings("rawtypes")
    protected KafkaSpoutWrapper createKafkaSpout(Map conf, TopologyContext context, SpoutOutputCollector collector, final String topic,
                                                 String schemeClsName, SpoutSpec spoutSpec, Map<String, StreamDefinition> sds) throws Exception{
        String kafkaBrokerZkQuorum = config.getString("spout.kafkaBrokerZkQuorum");
        BrokerHosts hosts = null;
        if (config.hasPath("spout.kafkaBrokerZkBasePath")) {
            hosts = new ZkHosts(kafkaBrokerZkQuorum, config.getString("spout.kafkaBrokerZkBasePath"));
        } else {
            hosts = new ZkHosts(kafkaBrokerZkQuorum);
        }
        String transactionZkRoot = DEFAULT_STORM_KAFKA_TRANSACTION_ZK_ROOT;
        if(config.hasPath("spout.stormKafkaTransactionZkPath")) {
            transactionZkRoot = config.getString("spout.stormKafkaTransactionZkPath");
        }
        // write partition offset etc. into zkRoot+id, see PartitionManager.committedPath
        String zkStateTransactionRelPath = DEFAULT_STORM_KAFKA_TRANSACTION_ZK_RELATIVE_PATH;
        if(config.hasPath("spout.stormKafkaEagleConsumer")){
            zkStateTransactionRelPath = config.getString("spout.stormKafkaEagleConsumer");
        }
        SpoutConfig spoutConfig = new SpoutConfig(hosts, topic, transactionZkRoot, zkStateTransactionRelPath + "/" + topic + "/" + topologyId);
        // transaction zkServers
        boolean stormKafkaUseSameZkQuorumWithKafkaBroker = config.getBoolean("spout.stormKafkaUseSameZkQuorumWithKafkaBroker");
        if(stormKafkaUseSameZkQuorumWithKafkaBroker){
            ZkServerPortUtils utils = new ZkServerPortUtils(kafkaBrokerZkQuorum);
            spoutConfig.zkServers = utils.getZkHosts();
            spoutConfig.zkPort = utils.getZkPort();
        }else{
            ZkServerPortUtils utils = new ZkServerPortUtils(config.getString("spout.stormKafkaTransactionZkQuorum"));
            spoutConfig.zkServers = utils.getZkHosts();
            spoutConfig.zkPort = utils.getZkPort();
        }
        // transaction update interval
        spoutConfig.stateUpdateIntervalMs = config.getLong("spout.stormKafkaStateUpdateIntervalMs");
        // Kafka fetch size
        spoutConfig.fetchSizeBytes = config.getInt("spout.stormKafkaFetchSizeBytes");
        // "startOffsetTime" is for test usage, prod should not use this
        if (config.hasPath("spout.stormKafkaStartOffsetTime")) {
            spoutConfig.startOffsetTime = config.getInt("spout.stormKafkaStartOffsetTime");
        }

        spoutConfig.scheme = new SchemeAsMultiScheme(SchemeBuilder.buildFromClsName(schemeClsName, topic, conf));
        KafkaSpoutWrapper wrapper = new KafkaSpoutWrapper(spoutConfig, kafkaSpoutMetric);
        SpoutOutputCollectorWrapper collectorWrapper = new SpoutOutputCollectorWrapper(this, collector, topic, spoutSpec, numOfRouterBolts, sds,this.serializer);
        wrapper.open(conf, context, collectorWrapper);
        
        if (LOG.isInfoEnabled()) {
            LOG.info("create and open kafka wrapper: topic {}, scheme class{} ", topic, schemeClsName);
        }
        return wrapper;
    }

    @Override
    public StreamDefinition getStreamDefinition(String streamId) {
        return sds.get(streamId);
    }

    /**
     * utility to get list of zkServers and zkPort.(It is assumed that zkPort is same for all zkServers as storm-kafka library requires this though it is not efficient)
     */
    private static class ZkServerPortUtils{
        private List<String> zkHosts = new ArrayList<>();
        private Integer zkPort;
        public ZkServerPortUtils(String zkQuorum){
            String[] zkConnections = zkQuorum.split(",");
            for (String zkConnection : zkConnections) {
                zkHosts.add(zkConnection.split(":")[0]);
            }
            zkPort = Integer.valueOf(zkConnections[0].split(":")[1]);
        }

        public List<String> getZkHosts(){
            return zkHosts;
        }

        public Integer getZkPort(){
            return zkPort;
        }
    }

    protected void removeKafkaSpout(KafkaSpoutWrapper wrapper){
        try {
            wrapper.close();
        } catch (Exception e) {
            LOG.error("Close wrapper failed. Ignore and continue!", e);
        }
    }
}