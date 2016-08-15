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

import static org.apache.eagle.alert.engine.utils.SpecUtils.getTopicsByClient;

import org.apache.eagle.alert.coordination.model.AlertBoltSpec;
import org.apache.eagle.alert.coordination.model.PublishSpec;
import org.apache.eagle.alert.coordination.model.RouterSpec;
import org.apache.eagle.alert.coordination.model.SpoutSpec;
import org.apache.eagle.alert.engine.coordinator.StreamDefinition;
import org.apache.eagle.alert.service.SpecMetadataServiceClientImpl;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.kafka.HasOffsetRanges;
import org.apache.spark.streaming.kafka.OffsetRange;
import com.typesafe.config.Config;
import kafka.message.MessageAndMetadata;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

public class FetchSpecAndTopicFunction implements Function<JavaRDD<MessageAndMetadata<String, String>>, JavaRDD<MessageAndMetadata<String, String>>> {

    private SpecMetadataServiceClientImpl client;
    private AtomicReference<OffsetRange[]> offsetRanges = new AtomicReference<OffsetRange[]>();
    private AtomicReference<SpoutSpec> spoutSpecRef = new AtomicReference<SpoutSpec>();
    private AtomicReference<RouterSpec> routerSpecRef = new AtomicReference<RouterSpec>();
    private AtomicReference<AlertBoltSpec> alertBoltSpecRef = new AtomicReference<AlertBoltSpec>();
    private AtomicReference<PublishSpec> publishSpecRef = new AtomicReference<PublishSpec>();
    private AtomicReference<Map<String, StreamDefinition>> sdsRef = new AtomicReference<Map<String, StreamDefinition>>();
    private AtomicReference<Set<String>> topicsRef = new AtomicReference<Set<String>>();

    public FetchSpecAndTopicFunction(AtomicReference<OffsetRange[]> offsetRanges, AtomicReference<SpoutSpec> spoutSpecRef,
                                     AtomicReference<Map<String, StreamDefinition>> sdsRef, AtomicReference<AlertBoltSpec> alertBoltSpecRef,
                                     AtomicReference<PublishSpec> publishSpecRef, AtomicReference<RouterSpec> routerSpecRef,
                                     AtomicReference<Set<String>> topicsRef, Config config) {
        this.offsetRanges = offsetRanges;
        this.spoutSpecRef = spoutSpecRef;
        this.alertBoltSpecRef = alertBoltSpecRef;
        this.publishSpecRef = publishSpecRef;
        this.topicsRef = topicsRef;
        this.routerSpecRef = routerSpecRef;
        this.client = new SpecMetadataServiceClientImpl(config);
        this.sdsRef = sdsRef;

    }

    @Override
    public JavaRDD<MessageAndMetadata<String, String>> call(JavaRDD<MessageAndMetadata<String, String>> rdd) throws Exception {
        spoutSpecRef.set(client.getSpoutSpec());
        routerSpecRef.set(client.getRouterSpec());
        alertBoltSpecRef.set(client.getAlertBoltSpec());
        publishSpecRef.set(client.getPublishSpec());
        sdsRef.set(client.getSds());
        topicsRef.set(getTopicsByClient(client));

        OffsetRange[] offsets = ((HasOffsetRanges) rdd.rdd()).offsetRanges();
        offsetRanges.set(offsets);
        return rdd;
    }

}
