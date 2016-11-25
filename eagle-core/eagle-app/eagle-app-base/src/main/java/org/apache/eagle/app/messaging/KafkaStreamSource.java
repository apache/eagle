/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.eagle.app.messaging;

import backtype.storm.spout.Scheme;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.kafka.*;

import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

public class KafkaStreamSource extends StormStreamSource<KafkaStreamSinkConfig> {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaStreamSource.class);
    private KafkaSpout spout;

    @Override
    public void prepare(String streamId, KafkaStreamSinkConfig config) {
        this.spout = createKafkaSpout(config);
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.spout.open(conf, context, collector);
    }

    @Override
    public void nextTuple() {
        this.spout.nextTuple();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        this.spout.declareOutputFields(declarer);
    }

    @Override
    public void close() {
        this.spout.close();
    }

    @Override
    public void activate() {
        this.spout.activate();
    }

    @Override
    public void deactivate() {
        this.spout.deactivate();
    }

    @Override
    public void ack(Object msgId) {
        this.spout.ack(msgId);
    }

    @Override
    public void fail(Object msgId) {
        this.spout.fail(msgId);
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return this.spout.getComponentConfiguration();
    }

    // ----------------
    //  Helper Methods
    // ----------------

    private static KafkaSpout createKafkaSpout(KafkaStreamSinkConfig config) {

        // the following is for fetching data from one topic
        // Kafka topic
        String topic = config.getTopicId();
        // Kafka broker zk connection
        String zkConnString = config.getBrokerZkQuorum();
        // Kafka fetch size
        int fetchSize = config.getFetchSize();
        LOG.info(String.format("Use topic : %s, zkQuorum : %s , fetchSize : %d", topic, zkConnString, fetchSize));

        /*
         the following is for recording offset for processing the data
         the zk path to store current offset is comprised of the following
         offset zkPath = zkRoot + "/" + topic + "/" + consumerGroupId + "/" + partition_Id

         consumerGroupId is for differentiating different consumers which consume the same topic
        */
        // transaction zkRoot
        String zkRoot = config.getTransactionZKRoot();
        // Kafka consumer group id
        String groupId = config.getConsumerGroupId();
        String brokerZkPath = config.getBrokerZkPath();

        BrokerHosts hosts;
        if (brokerZkPath == null) {
            hosts = new ZkHosts(zkConnString);
        } else {
            hosts = new ZkHosts(zkConnString, brokerZkPath);
        }

        SpoutConfig spoutConfig = new SpoutConfig(hosts,
            topic,
            zkRoot + "/" + topic,
            groupId);

        // transaction zkServers to store kafka consumer offset. Default to use storm zookeeper
        if (config.getTransactionZkServers() != null) {
            String[] txZkServers = config.getTransactionZkServers().split(",");
            spoutConfig.zkServers = Arrays.asList(txZkServers).stream().map(server -> server.split(":")[0]).collect(Collectors.toList());
            spoutConfig.zkPort = Integer.parseInt(txZkServers[0].split(":")[1]);
            LOG.info("txZkServers:" + spoutConfig.zkServers + ", zkPort:" + spoutConfig.zkPort);
        }

        // transaction update interval
        spoutConfig.stateUpdateIntervalMs = config.getTransactionStateUpdateMS();
        // Kafka fetch size
        spoutConfig.fetchSizeBytes = fetchSize;
        spoutConfig.startOffsetTime = kafka.api.OffsetRequest.LatestTime();

        // "startOffsetTime" is for test usage, prod should not use this
        if (config.getStartOffsetTime() >= 0) {
            spoutConfig.startOffsetTime = config.getStartOffsetTime();
        }
        // "forceFromStart" is for test usage, prod should not use this
        if (config.isForceFromStart()) {
            spoutConfig.startOffsetTime = kafka.api.OffsetRequest.EarliestTime();
        }

        if (config.getSchemaClass() != null ) {
            try {
                Scheme s = config.getSchemaClass().newInstance();
                spoutConfig.scheme = new SchemeAsMultiScheme(s);
            } catch (Exception ex) {
                LOG.error("Error instantiating scheme object");
                throw new IllegalStateException(ex);
            }
        } else {
            String err = "schemaClass is null";
            LOG.error(err);
            throw new IllegalStateException(err);
        }

        return new KafkaSpout(spoutConfig);
    }
}