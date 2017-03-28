/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.eagle.alert.engine.spark.model;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import kafka.common.TopicAndPartition;
import org.apache.spark.streaming.kafka.KafkaCluster;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class KafkaClusterInfo implements Serializable {
    private Set<String> topics = Sets.newHashSet();
    private String zkQuorum;
    private String brokerList;
    private KafkaCluster kafkaCluster;
    private Map<String, String> kafkaParams = Maps.newHashMap();
    private HashMap<TopicAndPartition, Long> offsets;

    public KafkaClusterInfo(String zkQuorum) {
        this.zkQuorum = zkQuorum;
    }

    public Set<String> getTopics() {
        return topics;
    }

    public void setTopics(Set<String> topics) {
        this.topics = topics;
    }

    public String getZkQuorum() {
        return zkQuorum;
    }

    public void setZkQuorum(String zkQuorum) {
        this.zkQuorum = zkQuorum;
    }

    public String getBrokerList() {
        return brokerList;
    }

    public void setBrokerList(String brokerList) {
        this.brokerList = brokerList;
    }

    public KafkaCluster getKafkaCluster() {
        return kafkaCluster;
    }

    public void setKafkaCluster(KafkaCluster kafkaCluster) {
        this.kafkaCluster = kafkaCluster;
    }

    public Map<String, String> getKafkaParams() {
        return kafkaParams;
    }

    public void setKafkaParams(Map<String, String> kafkaParams) {
        this.kafkaParams = kafkaParams;
    }

    public HashMap<TopicAndPartition, Long> getOffsets() {
        return offsets;
    }

    public void setOffsets(Map<TopicAndPartition, Long> offsets) {
        this.offsets = new HashMap<>(offsets);
    }

    public void addTopic(String topic) {
        this.topics.add(topic);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        KafkaClusterInfo kafkaInfo = (KafkaClusterInfo) o;


        /*if (!topic.equals(kafkaInfo.topic)) {
            return false;
        }*/
        String[] zkQuorumArray = zkQuorum.split(",");
        String[] zkQuorunArray2 = kafkaInfo.zkQuorum.split(",");
        Arrays.sort(zkQuorumArray);
        Arrays.sort(zkQuorunArray2);
        return Arrays.equals(zkQuorumArray, zkQuorunArray2);
    }

    @Override
    public int hashCode() {
        // int result = topic.hashCode();
        String[] zkQuorumArray = zkQuorum.split(",");
        Arrays.sort(zkQuorumArray);
        int result = 0;
        for (String zkQuorum : zkQuorumArray) {
            result = 31 * result + zkQuorum.hashCode();
        }
        return result;
    }


}
