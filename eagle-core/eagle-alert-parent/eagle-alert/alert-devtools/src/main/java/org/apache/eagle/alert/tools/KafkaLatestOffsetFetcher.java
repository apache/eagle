/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.eagle.alert.tools;


import kafka.api.PartitionOffsetRequestInfo;
import kafka.common.TopicAndPartition;
import kafka.javaapi.OffsetResponse;
import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;
import kafka.javaapi.TopicMetadataRequest;
import kafka.javaapi.consumer.SimpleConsumer;

import java.util.*;

public class KafkaLatestOffsetFetcher {

    private List<String> brokerList;
    private int port;

    public KafkaLatestOffsetFetcher(String brokerList) {
        this.brokerList = new ArrayList<>();
        String[] brokers = brokerList.split(",");
        for (String broker : brokers) {
            this.brokerList.add(broker.split(":")[0]);
        }
        this.port = Integer.valueOf(brokers[0].split(":")[1]);
    }

    public Map<Integer, Long> fetch(String topic, int partitionCount) {
        Map<Integer, PartitionMetadata> metadatas = fetchPartitionMetadata(brokerList, port, topic, partitionCount);
        Map<Integer, Long> ret = new HashMap<>();
        for (int partition = 0; partition < partitionCount; partition++) {
            PartitionMetadata metadata = metadatas.get(partition);
            if (metadata == null || metadata.leader() == null) {
                ret.put(partition, -1L);
                //throw new RuntimeException("Can't find Leader for Topic and Partition. Exiting");
            }
            String leadBroker = metadata.leader().host();
            String clientName = "Client_" + topic + "_" + partition;
            SimpleConsumer consumer = new SimpleConsumer(leadBroker, port, 100000, 64 * 1024, clientName);
            long latestOffset = getLatestOffset(consumer, topic, partition, clientName);
            if (consumer != null) {
                consumer.close();
            }
            ret.put(partition, latestOffset);
        }
        return ret;
    }

    public long getLatestOffset(SimpleConsumer consumer, String topic, int partition, String clientName) {
        TopicAndPartition topicAndPartition = new TopicAndPartition(topic, partition);
        Map<TopicAndPartition, kafka.api.PartitionOffsetRequestInfo> requestInfo = new HashMap<>();
        requestInfo.put(topicAndPartition, new PartitionOffsetRequestInfo(kafka.api.OffsetRequest.LatestTime(), 1));
        kafka.javaapi.OffsetRequest request = new kafka.javaapi.OffsetRequest(requestInfo, kafka.api.OffsetRequest.CurrentVersion(), clientName);
        OffsetResponse response = consumer.getOffsetsBefore(request);
        if (response.hasError()) {
            throw new RuntimeException("Error fetching data offset from the broker. Reason: " + response.errorCode(topic, partition));
        }
        long[] offsets = response.offsets(topic, partition);
        return offsets[0];
    }

    private Map<Integer, PartitionMetadata> fetchPartitionMetadata(List<String> brokerList, int port, String topic, int partitionCount) {
        Map<Integer, PartitionMetadata> partitionMetadata = new HashMap<>();
        for (String broker : brokerList) {
            SimpleConsumer consumer = null;
            try {
                consumer = new SimpleConsumer(broker, port, 100000, 64 * 1024, "leaderLookup");
                List<String> topics = Collections.singletonList(topic);
                TopicMetadataRequest req = new TopicMetadataRequest(topics);
                kafka.javaapi.TopicMetadataResponse resp = consumer.send(req);

                List<TopicMetadata> metaData = resp.topicsMetadata();
                for (TopicMetadata item : metaData) {
                    for (PartitionMetadata part : item.partitionsMetadata()) {
                        partitionMetadata.put(part.partitionId(), part);
                    }
                }
                if (partitionMetadata.size() == partitionCount) {
                    break;
                }
            } catch (Exception e) {
                throw new RuntimeException("Error communicating with Broker [" + broker + "] " + "to find Leader for [" + topic + "] Reason: ", e);
            } finally {
                if (consumer != null) {
                    consumer.close();
                }
            }
        }
        return partitionMetadata;
    }
}
