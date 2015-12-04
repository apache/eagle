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
package org.apache.eagle.security.userprofile;

import com.typesafe.config.Config;
import org.apache.eagle.dataproc.impl.storm.kafka.KafkaSourcedSpoutProvider;
import org.apache.eagle.datastream.*;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class UserProfileDetectionStreamMain {
    public static void main(String[] args) throws Exception{
        StormExecutionEnvironment env = ExecutionEnvironments.getStorm(args);
        env.from(new KafkaSourcedSpoutProvider()).renameOutputFields(1).as("kafkaMsgConsumer")
                .flatMap(new AuditLogTransformer()).as("transformer")     // [user,map]
                .groupBy(Arrays.asList(0))                                      // group by [user]
                .flatMap(new UserProfileAggregatorExecutor()).as("aggregator")
                .alertWithConsumer(UserProfileDetectionConstants.USER_ACTIVITY_AGGREGATION_STREAM,
                        UserProfileDetectionConstants.USER_PROFILE_ANOMALY_DETECTION_EXECUTOR); // alert
        env.execute();
    }

    private static class AuditLogTransformer extends JavaStormStreamExecutor2<String,Map<String,Object>> {
        @Override
        public void prepareConfig(Config config) {}

        @Override
        public void init() {}

        @Override
        public void flatMap(List<Object> input, Collector<Tuple2<String, Map<String, Object>>> collector) {
            Map<String, Object> auditLog = (Map<String, Object>) input.get(0);
            collector.collect(new Tuple2<>((String)auditLog.get("user"),auditLog));
        }
    }
}