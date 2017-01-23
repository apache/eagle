package org.apache.eagle.alert.engine.spark.model;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import kafka.common.TopicAndPartition;
import org.apache.spark.streaming.kafka.KafkaCluster;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;

/**
 * Created by koone on 2017/1/11.
 */
public class KafkaClusterInfo implements Serializable {
    private Set<String> topics = Sets.newHashSet();
    private String zkQuorum;
    private String brokerList;
    private KafkaCluster kafkaCluster;
    private Map<String, String> kafkaParams = Maps.newHashMap();
    private scala.collection.immutable.Map<TopicAndPartition, scala.Long> offsets;

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

    public scala.collection.immutable.Map<TopicAndPartition, scala.Long> getOffsets() {
        return offsets;
    }

    public void setOffsets(scala.collection.immutable.Map<TopicAndPartition, scala.Long> offsets) {
        this.offsets = offsets;
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
