package org.apache.eagle.alert.engine.utils;

import kafka.admin.AdminUtils;
import kafka.common.TopicAndPartition;
import org.I0Itec.zkclient.ZkClient;
import org.apache.eagle.alert.engine.spark.model.KafkaClusterInfo;
import org.apache.spark.streaming.kafka.KafkaCluster;
import scala.collection.JavaConverters;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Created by koone on 2017/1/12.
 */
public class EagleKafkaUtils4MultiKafka {

    private static Long ZK_TIMEOUT_MSEC = TimeUnit.MILLISECONDS.convert(30, TimeUnit.SECONDS);

    public static void fillInLatestOffsets(Map<KafkaClusterInfo, Set<String>> clusterInfoMap, Map<KafkaClusterInfo, Map<TopicAndPartition, Long>> fromOffsetsClusterMap, String groupId) {
        if (clusterInfoMap.isEmpty()) {
            throw new IllegalArgumentException("clusterInfo should not be null");
        }
        for (KafkaClusterInfo clusterInfo : clusterInfoMap.keySet()) {
            Set<String> topics = clusterInfoMap.get(clusterInfo);
            ZkClient zkClient = new ZkClient(clusterInfo.getZkQuorum(), ZK_TIMEOUT_MSEC.intValue(), ZK_TIMEOUT_MSEC.intValue());
            Set<String> effectiveTopics = topics.stream().filter(topic -> AdminUtils.topicExists(zkClient, topic)).collect(Collectors.toSet());
            KafkaCluster cluster = clusterInfo.getKafkaCluster();
            scala.collection.Set<TopicAndPartition> tpSet = cluster.getPartitions(JavaConverters.asScalaSetConverter(effectiveTopics).asScala().toSet()).right().get();
            // cluster.getConsumerOffsets(groupId, tpSet);

        }

    }
}
