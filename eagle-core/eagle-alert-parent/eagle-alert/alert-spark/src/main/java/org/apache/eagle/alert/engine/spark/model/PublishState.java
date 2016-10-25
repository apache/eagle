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

import org.apache.eagle.alert.engine.coordinator.Publishment;

import org.apache.eagle.alert.engine.spark.accumulator.MapAccum;
import org.apache.spark.Accumulator;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

public class PublishState implements Serializable {
    private static final Logger LOG = LoggerFactory.getLogger(PublishState.class);
    private AtomicReference<Map<String, Map<String, Publishment>>> cachedPublishmentsRef = new AtomicReference<>();

    private Accumulator<Map<String, Map<String, Publishment>>> cachedPublishmentsAccum;

    public PublishState(JavaStreamingContext jssc) {
        Accumulator<Map<String, Map<String, Publishment>>> cachedPublishmentsAccum = jssc.sparkContext().accumulator(new HashMap<>(), "cachedPublishmentsAccum", new MapAccum());
        this.cachedPublishmentsAccum = cachedPublishmentsAccum;
    }

    public void recover() {
        cachedPublishmentsRef.set(cachedPublishmentsAccum.value());
        LOG.debug("---------cachedPublishmentsRef----------" + cachedPublishmentsRef.get());
    }

    public void store(String streamId, Map<String, Publishment> cachedPublishments) {

        if (!cachedPublishments.isEmpty()) {
            Map<String, Map<String, Publishment>> newCachedPublishments = new HashMap<>();
            newCachedPublishments.put(streamId, cachedPublishments);
            cachedPublishmentsAccum.add(newCachedPublishments);
        }

    }

    public Map<String, Publishment> getCachedPublishmentsByStreamId(String streamId) {
        Map<String, Map<String, Publishment>> streamIdToCachedPublishments = cachedPublishmentsRef.get();
        LOG.debug("---PublishState----getCachedPublishmentsByStreamId----------" + (streamIdToCachedPublishments));
        Map<String, Publishment> cachedPublishments = streamIdToCachedPublishments.get(streamId);
        if (cachedPublishments == null) {
            cachedPublishments = new HashMap<>();
        }
        return cachedPublishments;
    }
}
