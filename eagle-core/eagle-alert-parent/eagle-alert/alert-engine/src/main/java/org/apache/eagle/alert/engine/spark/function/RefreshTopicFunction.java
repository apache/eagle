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

import kafka.common.TopicAndPartition;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.kafka.EagleKafkaUtils;
import org.apache.spark.streaming.kafka.KafkaCluster;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Predef;
import scala.Tuple2;
import scala.collection.JavaConversions;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

public class RefreshTopicFunction implements Function<scala.collection.immutable.Map<TopicAndPartition, Object>, scala.collection.immutable.Map<TopicAndPartition, Object>> {

    private static final Logger LOG = LoggerFactory.getLogger(RefreshTopicFunction.class);
    private AtomicReference<Set<String>> topicsRef = new AtomicReference<Set<String>>();
    private String groupId;
    private KafkaCluster kafkaCluster;
    private String zkServers;

    public RefreshTopicFunction(AtomicReference<Set<String>> topicsRef, String groupId, KafkaCluster kafkaCluster, String zkServers) {
        this.topicsRef = topicsRef;
        this.groupId = groupId;
        this.kafkaCluster = kafkaCluster;
        this.zkServers = zkServers;
    }

    @Override
    public scala.collection.immutable.Map<TopicAndPartition, Object> call(scala.collection.immutable.Map<TopicAndPartition, Object> oldOffset) throws Exception {

        LOG.info("-------offset---OLD--" + oldOffset);

        Map<TopicAndPartition, Object> oldOffsetJavaMap = JavaConversions.mapAsJavaMap(oldOffset);

        Map<TopicAndPartition, Long> oldOffsetWithTypeMap = new HashMap<>(oldOffsetJavaMap.size());
        oldOffsetJavaMap.forEach((topicAndPartition, offset) -> {
            oldOffsetWithTypeMap.put(topicAndPartition, Long.parseLong(offset.toString()));
        });

        Map<TopicAndPartition, Object> newOffset = EagleKafkaUtils.refreshOffsets(topicsRef.get(),
                oldOffsetWithTypeMap,
                this.groupId,
                this.kafkaCluster,
                this.zkServers);
        if (newOffset == null) {
            LOG.info("first init app so remain the oldOffset");
            return oldOffset;
        }
        scala.collection.mutable.Map<TopicAndPartition, Object> mutableNewOffsetJavaMap = JavaConversions
                .mapAsScalaMap(newOffset);
        scala.collection.immutable.Map<TopicAndPartition, Object> immutableNewOffsetJavaMap = mutableNewOffsetJavaMap
                .toMap(new Predef.$less$colon$less<Tuple2<TopicAndPartition, Object>, Tuple2<TopicAndPartition, Object>>() {
                    public Tuple2<TopicAndPartition, Object> apply(
                            Tuple2<TopicAndPartition, Object> v1) {
                        return v1;
                    }
                });
        LOG.info("-------offset---NEW----" + immutableNewOffsetJavaMap);
        return immutableNewOffsetJavaMap;
    }
}
