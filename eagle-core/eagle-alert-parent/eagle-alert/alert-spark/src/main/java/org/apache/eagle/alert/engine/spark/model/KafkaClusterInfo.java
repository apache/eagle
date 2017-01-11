package org.apache.eagle.alert.engine.spark.model;

import org.apache.spark.streaming.kafka.KafkaCluster;

import java.io.Serializable;
import java.util.Arrays;

/**
 * Created by koone on 2017/1/11.
 */
public class KafkaClusterInfo implements Serializable {
    private String topic;
    private String zkQuorum;
    private String brokerList;
    private KafkaCluster kafkaCluster;

    public KafkaClusterInfo(String topic, String zkQuorum) {
        this.topic = topic;
        this.zkQuorum = zkQuorum;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
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
