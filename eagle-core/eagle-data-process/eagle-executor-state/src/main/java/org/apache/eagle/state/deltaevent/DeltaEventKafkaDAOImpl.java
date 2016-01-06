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

package org.apache.eagle.state.deltaevent;

import com.typesafe.config.Config;
import kafka.javaapi.TopicMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;

/**
 * writeState delta events to kafka
 * 1. topic should be a format like executor_state_${applicationId}
 * 2. delta event should be keyed by executorId and partitionId
 *
 * need consider close KafkaProducer and release I/O resource used by KafkaProducer
 */
public class DeltaEventKafkaDAOImpl implements DeltaEventDAO {
    private static final Logger LOG = LoggerFactory.getLogger(DeltaEventKafkaDAOImpl.class);
    private String topic;
    private KafkaProducer producer;
    private int partitionNum;
    private String site;
    private String applicationId;
    private String elementId;
    private KafkaReadWithOffsetRange offsetReader;

    public DeltaEventKafkaDAOImpl(Config config, String elementId){
        // create KafkaProducer
        this.elementId = elementId;
        this.site = config.getString("eagleProps.site");
        this.applicationId = config.getString("eagleProps.dataSource");
        String topicBase = config.getString("eagleProps.executorState.topicBase");
        topic = topicBase + "_" + site + "_" + applicationId;
        Map producerConfigs = config.getObject("eagleProps.executorState.deltaEventKafkaProducerConfig").unwrapped();
        producer = new KafkaProducer(producerConfigs);

        // fetch number of partitions
        List<String> brokerList = Arrays.asList(config.getString("eagleProps.executorState.brokerList").split(","));
        int brokerPort = config.getInt("eagleProps.executorState.brokerPort");
        TopicMetadata topicMetadata = KafkaMetadataUtils.metadataForTopic(brokerList, brokerPort, "eagleExecutorState_leaderLookup", topic);
        int numPartitions = topicMetadata.partitionsMetadata().size();
        LOG.info("topic: " + topic + ", total number of partitions: " + numPartitions);
        DeltaEventKey key = new DeltaEventKey();
        key.setSite(site);
        key.setElementId(elementId);
        key.setApplicationId(applicationId);
        partitionNum = Math.abs(key.hashCode()) % numPartitions;

        // initialize kafka reader
        String deserializerCls = config.getString("eagleProps.executorState.deltaEventKafkaConsumerConfig.valueDeserializer");
        try {
            offsetReader = new KafkaReadWithOffsetRange(brokerList,
                    brokerPort,
                    topic,
                    partitionNum,
                    (Deserializer)Class.forName(deserializerCls).newInstance()
            );
        }catch(Exception ex){
            LOG.error("fail creating kafka reader", ex);
            throw new RuntimeException(ex);
        }
    }

    @Override
    public long write(Object event) throws Exception {
        DeltaEventKey key = new DeltaEventKey();
        key.setSite(site);
        key.setElementId(elementId);
        key.setApplicationId(applicationId);
        DeltaEventValue value = new DeltaEventValue();
        value.setElementId(elementId);
        value.setEvent(event);
        ProducerRecord<DeltaEventKey, Object> record = new ProducerRecord<DeltaEventKey, Object>(topic, partitionNum, key, value);
        Future<RecordMetadata> future = producer.send(record);
        RecordMetadata recordMetadata = future.get();
        return recordMetadata.offset();
    }

    @Override
    public void load(long startOffset, DeltaEventReplayCallback callback) throws Exception{
        offsetReader.readUntilMaxOffset(startOffset, callback);
    }

    @Override
    public void close() throws IOException {
        this.producer.close();
    }
}
