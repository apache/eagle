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
package org.apache.eagle.hadoop.metric;

import backtype.storm.spout.SchemeAsMultiScheme;
import com.typesafe.config.Config;
import org.apache.eagle.dataproc.impl.storm.kafka.KafkaSourcedSpoutProvider;
import org.apache.eagle.dataproc.impl.storm.kafka.KafkaSourcedSpoutScheme;
import org.apache.eagle.datastream.ExecutionEnvironments;
import org.apache.eagle.datastream.core.StreamProducer;
import org.apache.eagle.datastream.storm.StormExecutionEnvironment;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Created on 1/12/16.
 */
public class NameNodeLagMonitor {

    public static void main(String[] args) {
        StormExecutionEnvironment env = ExecutionEnvironments.get(args, StormExecutionEnvironment.class);
        String streamName = "s";
        StreamProducer sp = env.fromSpout(createProvider(env.getConfig())).withOutputFields(2).parallelism(1).nameAs(streamName);
        sp.alertWithConsumer(streamName, "hadoopJmxMetricAlertExecutor");

        env.execute();
    }

    // create a tuple kafka source
    private static KafkaSourcedSpoutProvider createProvider(Config config) {
        String deserClsName = config.getString("dataSourceConfig.deserializerClass");
        final KafkaSourcedSpoutScheme scheme = new KafkaSourcedSpoutScheme(deserClsName, config) {
            @Override
            public List<Object> deserialize(byte[] ser) {
                Object tmp = deserializer.deserialize(ser);
                Map<String, Object> map = (Map<String, Object>)tmp;
                if(tmp == null) return null;
                // this is the key to be grouped by
                return Arrays.asList(String.format("%s-%s", map.get("host"), map.get("metric")), tmp);
            }
        };

        KafkaSourcedSpoutProvider provider = new KafkaSourcedSpoutProvider() {
            @Override
            public SchemeAsMultiScheme getStreamScheme(String deserClsName, Config context) {
                return new SchemeAsMultiScheme(scheme);
            }
        };
        return provider;
    }
}
