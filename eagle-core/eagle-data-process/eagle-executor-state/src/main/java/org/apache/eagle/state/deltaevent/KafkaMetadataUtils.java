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

import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;
import kafka.javaapi.TopicMetadataRequest;
import kafka.javaapi.consumer.SimpleConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;

/**
 * Some utility methods to get Kafka related information including
 * 1. metadata for one topic
 * 2. metadata for one partition of a topic
 */
public class KafkaMetadataUtils {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaMetadataUtils.class);

    public static TopicMetadata metadataForTopic(List<String> brokers, int port, String clientId, String topic){
        for (String seed : brokers) {
            SimpleConsumer consumer = null;
            try {
                consumer = new SimpleConsumer(seed, port, 100000, 64 * 1024, clientId);
                List<String> topics = Collections.singletonList(topic);
                TopicMetadataRequest req = new TopicMetadataRequest(topics);
                kafka.javaapi.TopicMetadataResponse resp = consumer.send(req);
                List<TopicMetadata> metaData = resp.topicsMetadata();
                return metaData.get(0);
            } catch (Exception e) {
                LOG.error("Error communicating with Broker [" + seed + "] to find Leader for [" + topic + "] Reason: " + e);
            } finally {
                if (consumer != null) consumer.close();
            }
        }
        return null;
    }

    public static PartitionMetadata metadataForPartition(List<String> brokers, int port, String clientId, String topic, int partition){
        PartitionMetadata returnMetaData = null;
        TopicMetadata topicMetadata = metadataForTopic(brokers, port, clientId, topic);
        if(topicMetadata == null)
            return null;
        for (kafka.javaapi.PartitionMetadata part : topicMetadata.partitionsMetadata()) {
            if (part.partitionId() == partition) {
                returnMetaData = part;
                break;
            }
        }
        return returnMetaData;
    }
}
