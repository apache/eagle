/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.eagle.alert.engine.spark.function;

import org.apache.eagle.alert.engine.coordinator.Publishment;
import org.apache.eagle.alert.engine.model.AlertStreamEvent;

import kafka.common.TopicAndPartition;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.kafka.KafkaCluster;
import org.apache.spark.streaming.kafka.OffsetRange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Predef;
import scala.Tuple2;
import scala.collection.JavaConversions;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

public class Publisher implements VoidFunction<JavaPairRDD<String, AlertStreamEvent>> {

    private String alertPublishBoltName;
    private KafkaCluster kafkaCluster;
    private String groupId;
    private AtomicReference<OffsetRange[]> offsetRanges;
    private AtomicReference<Map<String, List<Publishment>>> publishmentsChangInfoRef;
    private static final Logger LOG = LoggerFactory.getLogger(Publisher.class);

    public Publisher(String alertPublishBoltName, KafkaCluster kafkaCluster, String groupId, AtomicReference<OffsetRange[]> offsetRanges,AtomicReference<Map<String, List<Publishment>>> publishmentsChangInfoRef) {
        this.alertPublishBoltName = alertPublishBoltName;
        this.kafkaCluster = kafkaCluster;
        this.groupId = groupId;
        this.offsetRanges = offsetRanges;
        this.publishmentsChangInfoRef = publishmentsChangInfoRef;
    }

    @Override
    public void call(JavaPairRDD<String, AlertStreamEvent> rdd) throws Exception {
        rdd.foreachPartition(new AlertPublisherBoltFunction(alertPublishBoltName,publishmentsChangInfoRef));
        updateOffset();
    }

    private void updateOffset() {
        for (OffsetRange eachOffsetRange : offsetRanges.get()) {
            TopicAndPartition topicAndPartition = new TopicAndPartition(eachOffsetRange.topic(), eachOffsetRange.partition());
            Map<TopicAndPartition, Object> topicAndPartitionObjectMap = new HashMap<>();
            topicAndPartitionObjectMap.put(topicAndPartition, eachOffsetRange.untilOffset());

            scala.collection.mutable.Map<TopicAndPartition, Object> map = JavaConversions.mapAsScalaMap(topicAndPartitionObjectMap);
            scala.collection.immutable.Map<TopicAndPartition, Object> scalatopicAndPartitionObjectMap =
                    map.toMap(new Predef.$less$colon$less<Tuple2<TopicAndPartition, Object>, Tuple2<TopicAndPartition, Object>>() {
                        public Tuple2<TopicAndPartition, Object> apply(Tuple2<TopicAndPartition, Object> v1) {
                            return v1;
                        }
                    });
            LOG.info("Updating offsets: {}", scalatopicAndPartitionObjectMap);
            kafkaCluster.setConsumerOffsets(groupId, scalatopicAndPartitionObjectMap);
        }
    }
}
