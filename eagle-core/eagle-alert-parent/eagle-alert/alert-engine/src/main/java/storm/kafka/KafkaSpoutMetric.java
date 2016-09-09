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
package storm.kafka;

import backtype.storm.metric.api.IMetric;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Since 5/18/16.
 * The original storm.kafka.KafkaSpout has some issues like the following
 * 1) can only support one single topic
 * 2) can only be initialized at open(), can't dynamically support another topic.
 */
public class KafkaSpoutMetric implements IMetric {
    @SuppressWarnings("unused")
    private static final Logger LOG = LoggerFactory.getLogger(KafkaSpoutMetric.class);
    private Map<String, KafkaSpoutMetricContext> metricContextMap = new ConcurrentHashMap<>();
    private Map<String, KafkaUtils.KafkaOffsetMetric> offsetMetricMap = new ConcurrentHashMap<>();

    public static class KafkaSpoutMetricContext {
        SpoutConfig spoutConfig;
        DynamicPartitionConnections connections;
        PartitionCoordinator coordinator;
    }

    public void addTopic(String topic, KafkaSpoutMetricContext context) {
        // construct KafkaOffsetMetric
        KafkaUtils.KafkaOffsetMetric kafkaOffsetMetric = new KafkaUtils.KafkaOffsetMetric(context.spoutConfig.topic, context.connections);
        metricContextMap.put(topic, context);
        offsetMetricMap.put(topic, kafkaOffsetMetric);
    }

    public void removeTopic(String topic) {
        metricContextMap.remove(topic);
        offsetMetricMap.remove(topic);
    }

    @SuppressWarnings( {"unchecked", "rawtypes"})
    @Override
    public Object getValueAndReset() {
        HashMap spoutMetric = new HashMap();
        for (Map.Entry<String, KafkaSpoutMetricContext> entry : metricContextMap.entrySet()) {
            // construct offset metric
            List<PartitionManager> pms = entry.getValue().coordinator.getMyManagedPartitions();
            Set<Partition> latestPartitions = new HashSet();
            for (PartitionManager pm : pms) {
                latestPartitions.add(pm.getPartition());
            }

            KafkaUtils.KafkaOffsetMetric offsetMetric = offsetMetricMap.get(entry.getKey());
            offsetMetric.refreshPartitions(latestPartitions);
            for (PartitionManager pm : pms) {
                offsetMetric.setLatestEmittedOffset(pm.getPartition(), pm.lastCompletedOffset());
            }
            Object o = offsetMetric.getValueAndReset();
            if (o != null) {
                ((HashMap) o).forEach(
                    (k, v) -> spoutMetric.put(k + "_" + entry.getKey(), v)
                );
            }

            // construct partition metric
            for (PartitionManager pm : pms) {
                pm.getMetricsDataMap().forEach(
                    (k, v) -> spoutMetric.put(k + "_" + entry.getKey(), v)
                );
            }
        }
        return spoutMetric;
    }
}
