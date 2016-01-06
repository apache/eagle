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

import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.common.ErrorMapping;
import kafka.common.TopicAndPartition;
import kafka.javaapi.*;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.message.MessageAndOffset;
import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.*;

/**
 * read kafka message from a specified start offset to current
 * does not support multiple threads
 */
public class KafkaReadWithOffsetRange {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaReadWithOffsetRange.class);

    private List<String> m_replicaBrokers = new ArrayList<String>();
    private List<String> seedBrokers;
    private int port;
    private String topic;
    private int partition;
    private Deserializer msgDeserializer;

    public KafkaReadWithOffsetRange(List<String> seedBrokers, int port, String topic, int partition, Deserializer msgDeserializer){
        this.seedBrokers = seedBrokers;
        this.port = port;
        this.topic = topic;
        this.partition = partition;
        this.msgDeserializer = msgDeserializer;
    }
    public void readUntilMaxOffset(long startOffset, DeltaEventReplayCallback callback) throws Exception{
        LOG.info("reading kafka message from offset " + startOffset + " for topic " + topic + ", partition " + partition);
        String clientName = "eagleExecutorState_" + topic + "_" + partition;
        PartitionMetadata metadata = findLeader(seedBrokers);
        if (metadata == null) {
            LOG.error("fail finding metadata for Topic and Partition: " + topic + "/" + partition);
            return;
        }
        if (metadata.leader() == null) {
            LOG.error("fail finding metadata for Topic and Partition: " + topic + "/" + partition);
            return;
        }
        String leadBroker = metadata.leader().host();
        SimpleConsumer consumer = new SimpleConsumer(leadBroker, port, 100000, 64 * 1024, clientName);
        long maxOffset = getLatestOffset(consumer, clientName);
        internalRead(consumer, leadBroker, clientName, callback, startOffset, maxOffset);
    }

    private void internalRead(SimpleConsumer consumer, String leadBroker, String clientName, DeltaEventReplayCallback callback, long startOffset, long maxOffset) throws Exception{
        long totalReplayed = 0L;
        long failedReplayed = 0L;
        int numErrors = 0;
        boolean finished = false;
        while (!finished) {
            // in some case, lead change will trigger consumer close and we need recreate consumer
            if(consumer == null){
                consumer = new SimpleConsumer(leadBroker, port, 100000, 64 * 1024, clientName);
            }
            FetchRequest req = new FetchRequestBuilder()
                    .clientId(clientName)
                    .addFetch(topic, partition, startOffset, 100000)
                    .build();
            FetchResponse fetchResponse = consumer.fetch(req);

            if (fetchResponse.hasError()) {
                numErrors++;
                short code = fetchResponse.errorCode(topic, partition);
                LOG.error("Error fetching data from the Broker:" + leadBroker + " Reason: " + code);
                if (numErrors > 5) break;
                if (code == ErrorMapping.OffsetOutOfRangeCode())  {
                    String errMsg = "try to read offset which is out of range";
                    LOG.error(errMsg);
                    throw new RuntimeException(errMsg);
                }
                consumer.close();
                consumer = null;
                leadBroker = findNewLeader(leadBroker);
                continue;
            }
            numErrors = 0;

            for (MessageAndOffset messageAndOffset : fetchResponse.messageSet(topic, partition)) {
                long currentOffset = messageAndOffset.offset();
                if (currentOffset < startOffset) {
                    LOG.info("Found an old offset: " + currentOffset + " Expecting: " + startOffset);
                    continue;
                }
                ByteBuffer payload = messageAndOffset.message().payload();
                byte[] bytes = new byte[payload.limit()];
                payload.get(bytes);
                DeltaEventValue event = null;
                try {
                    event = (DeltaEventValue)msgDeserializer.deserialize(topic, bytes);
                }catch(Exception ex){
                    failedReplayed++;
                    LOG.error("deserialization error, ignore this event");
                }
                totalReplayed++;
                if(event != null) {
                    callback.replay(event.getEvent());
                }
                if(currentOffset >= maxOffset-1){
                    finished = true;
                    break;
                }
            }
        }
        LOG.info("total replayed events " + totalReplayed, ", failedReplayed " + failedReplayed);
        if (consumer != null) consumer.close();
    }

    private PartitionMetadata findLeader(List<String> replicaBrokers) {
        PartitionMetadata returnMetaData = KafkaMetadataUtils.metadataForPartition(replicaBrokers, port, "eagleExecutorState_leaderLookup", topic, partition);
        if (returnMetaData != null) {
            m_replicaBrokers.clear();
            for (kafka.cluster.Broker replica : returnMetaData.replicas()) {
                m_replicaBrokers.add(replica.host());
            }
        }
        return returnMetaData;
    }

    private String findNewLeader(String oldLeader) throws Exception {
        for (int i = 0; i < 3; i++) {
            boolean goToSleep = false;
            PartitionMetadata metadata = findLeader(m_replicaBrokers);
            if (metadata == null) {
                goToSleep = true;
            } else if (metadata.leader() == null) {
                goToSleep = true;
            } else if (oldLeader.equalsIgnoreCase(metadata.leader().host()) && i == 0) {
                // first time through if the leader hasn't changed give ZooKeeper a second to recover
                // second time, assume the broker did recover before failover, or it was a non-Broker issue
                goToSleep = true;
            } else {
                return metadata.leader().host();
            }
            if (goToSleep) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ie) {
                }
            }
        }
        throw new Exception("Unable to find new leader after Broker failure. Exiting");
    }

    public long getLatestOffset(SimpleConsumer consumer, String clientId) {
        TopicAndPartition topicAndPartition = new TopicAndPartition(topic, partition);
        Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo = new HashMap<>();
        requestInfo.put(topicAndPartition, new PartitionOffsetRequestInfo(kafka.api.OffsetRequest.LatestTime(), 1));
        kafka.javaapi.OffsetRequest request = new kafka.javaapi.OffsetRequest(requestInfo, kafka.api.OffsetRequest.CurrentVersion(), clientId);
        OffsetResponse response = consumer.getOffsetsBefore(request);
        if (response.hasError()) {
            throw new RuntimeException("Error fetching data offset from the broker. Reason: " + response.errorCode(topic, partition) );
        }
        long[] offsets = response.offsets(topic, partition);
        return offsets[0];
    }
}
