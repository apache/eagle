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

package org.apache.eagle.alert.engine.spark.model;

import org.apache.eagle.alert.engine.coordinator.PublishPartition;
import org.apache.eagle.alert.engine.coordinator.Publishment;

import org.apache.eagle.alert.engine.spark.accumulator.MapToMapAccum;
import org.apache.eagle.alert.engine.spark.accumulator.MapToSetAccum;
import org.apache.spark.Accumulator;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

public class PublishState implements Serializable {
    private static final Logger LOG = LoggerFactory.getLogger(PublishState.class);
    private static final long serialVersionUID = 6228006353613787654L;
    private AtomicReference<Map<PublishPartition, Map<String, Publishment>>> cachedPublishmentsRef = new AtomicReference<>();
    private AtomicReference<Map<String, Set<PublishPartition>>> cachedPublishPartitionsRef = new AtomicReference<>();


    private Accumulator<Map<PublishPartition, Map<String, Publishment>>> cachedPublishmentsAccum;

    private Accumulator<Map<String, Set<PublishPartition>>> cachedPublishPartitionsAccum;


    public PublishState(JavaStreamingContext jssc) {
        Accumulator<Map<PublishPartition, Map<String, Publishment>>> cachedPublishmentsAccum = jssc.sparkContext().accumulator(new HashMap<>(), "cachedPublishmentsAccum", new MapToMapAccum());
        Accumulator<Map<String, Set<PublishPartition>>> cachedPublishPartitionsAccum = jssc.sparkContext().accumulator(new HashMap<>(), "cachedPublishPartitionsAccum", new MapToSetAccum());

        this.cachedPublishmentsAccum = cachedPublishmentsAccum;
        this.cachedPublishPartitionsAccum = cachedPublishPartitionsAccum;
    }

    public void recover() {
        cachedPublishmentsRef.set(cachedPublishmentsAccum.value());
        LOG.debug("---------cachedPublishmentsRef----------" + cachedPublishmentsRef.get());

        cachedPublishPartitionsRef.set(cachedPublishPartitionsAccum.value());
        LOG.debug("---------cachedPublishPartitionsRef----------" + cachedPublishPartitionsRef.get());

    }


    public void store(PublishPartition publishPartition, Map<String, Publishment> cachedPublishments) {

        if (!cachedPublishments.isEmpty()) {
            Map<PublishPartition, Map<String, Publishment>> newCachedPublishments = new HashMap<>();
            newCachedPublishments.put(publishPartition, cachedPublishments);
            cachedPublishmentsAccum.add(newCachedPublishments);
        }

    }

    public void storePublishPartitions(String boltId, Set<PublishPartition> cachedPublishPartitions) {

        if (!cachedPublishPartitions.isEmpty()) {
            Map<String, Set<PublishPartition>> newCachedPublishPartitions = new HashMap<>();
            newCachedPublishPartitions.put(boltId, cachedPublishPartitions);
            cachedPublishPartitionsAccum.add(newCachedPublishPartitions);
        }

    }

    public Map<String, Publishment> getCachedPublishmentsByPublishPartition(PublishPartition publishPartition) {
        Map<PublishPartition, Map<String, Publishment>> streamIdToCachedPublishments = cachedPublishmentsRef.get();
        LOG.debug("---PublishState----getCachedPublishmentsByStreamId----------" + (streamIdToCachedPublishments));
        Map<String, Publishment> cachedPublishments = streamIdToCachedPublishments.get(publishPartition);
        if (cachedPublishments == null) {
            cachedPublishments = new HashMap<>();
        }
        return cachedPublishments;
    }


    public Set<PublishPartition> getCachedPublishPartitionsByBoltId(String boltId) {
        Map<String, Set<PublishPartition>> boltIdToCachedPublishPartitions = cachedPublishPartitionsRef.get();
        LOG.debug("---PublishState----getCachedPublishPartitionsByBoltId----------" + (boltIdToCachedPublishPartitions));
        Set<PublishPartition> cachedPublishPartitions = boltIdToCachedPublishPartitions.get(boltId);
        if (cachedPublishPartitions == null) {
            cachedPublishPartitions = new HashSet<>();
        }
        return cachedPublishPartitions;
    }

}
