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

import com.typesafe.config.Config;
import kafka.common.TopicAndPartition;
import kafka.message.MessageAndMetadata;
import org.apache.eagle.alert.coordination.model.Kafka2TupleMetadata;
import org.apache.eagle.alert.engine.runner.UnitSparkTopologyRunner4MultiKafka;
import org.apache.eagle.alert.engine.spark.model.KafkaClusterInfo;
import org.apache.eagle.alert.service.IMetadataServiceClient;
import org.apache.eagle.alert.service.MetadataServiceClientImpl;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.EagleKafkaUtils4MultiKafka;
import org.apache.spark.streaming.kafka.OffsetRange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.atomic.AtomicReference;


public class RefreshUnionInputStreamFuntion implements Function2<JavaRDD<MessageAndMetadata<String, String>>, Time, JavaRDD<MessageAndMetadata<String, String>>> {


    private static final Logger LOG = LoggerFactory.getLogger(RefreshUnionInputStreamFuntion.class);
    private AtomicReference<Map<KafkaClusterInfo, OffsetRange[]>> offsetRangesClusterMapRef;
    private AtomicReference<Map<KafkaClusterInfo, Set<String>>> clusterInfoRef;
    private Config config;
    private JavaStreamingContext jssc;

    public RefreshUnionInputStreamFuntion(AtomicReference<Map<KafkaClusterInfo, Set<String>>> clusterInfoRef,
                                          Config config,
                                          JavaStreamingContext jssc,
                                          AtomicReference<Map<KafkaClusterInfo, OffsetRange[]>> offsetRangesClusterMapRef) {
        this.config = config;
        this.clusterInfoRef = clusterInfoRef;
        this.jssc = jssc;
        this.offsetRangesClusterMapRef = offsetRangesClusterMapRef;
    }

    @Override
    public JavaRDD<MessageAndMetadata<String, String>> call(JavaRDD<MessageAndMetadata<String, String>> rdd, Time time) throws Exception {
        Map<String, Map<String, String>> dataSourceProperties = new HashMap<>();
        List<Kafka2TupleMetadata> kafka2TupleMetadataList = new ArrayList<>();
        JavaRDD<MessageAndMetadata<String, String>> result = null;
        try {
            LOG.info("get topics By config");
            IMetadataServiceClient client = new MetadataServiceClientImpl(config);
            kafka2TupleMetadataList = client.listDataSources();
            for (Kafka2TupleMetadata ds : kafka2TupleMetadataList) {
                // ds.getProperties().put("spout.kafkaBrokerZkQuorum", "localhost:2181");
                dataSourceProperties.put(ds.getTopic(), ds.getProperties());
            }
            Map<KafkaClusterInfo, Set<String>> newClusterInfo = UnitSparkTopologyRunner4MultiKafka.getKafkaClustersByKafkaInfo(dataSourceProperties, clusterInfoRef.get());
            Map<KafkaClusterInfo, Set<String>> oldClusterInfo = clusterInfoRef.get();
            if (oldClusterInfo.keySet().containsAll(newClusterInfo.keySet()) && newClusterInfo.keySet().containsAll(oldClusterInfo.keySet())) {
                result = rdd;
            } else {
                clusterInfoRef.set(newClusterInfo);
                Map<KafkaClusterInfo, Map<TopicAndPartition, Long>> fromOffsetsClusterMapRef = new HashMap<>();
                EagleKafkaUtils4MultiKafka.fillInLatestOffsetsByCluster(clusterInfoRef.get(), fromOffsetsClusterMapRef, UnitSparkTopologyRunner4MultiKafka.groupId);
                List<JavaInputDStream<MessageAndMetadata<String, String>>> inputDStreams = UnitSparkTopologyRunner4MultiKafka.buldInputDstreamList(clusterInfoRef, fromOffsetsClusterMapRef, jssc,
                    offsetRangesClusterMapRef);
                // 4. union all kafka Dstream
                JavaInputDStream<MessageAndMetadata<String, String>> inputDStreamNew = inputDStreams.get(0);
                for (int i = 1; i < inputDStreams.size(); i++) {
                    inputDStreamNew.union(inputDStreams.get(i));
                }
                result = inputDStreamNew.compute(time);
            }
        } catch (Exception e) {
            LOG.error("refresh union input stream error :" + e.getMessage(), e);
        }
        return result;
    }
}