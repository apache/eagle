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
package org.apache.eagle.alert.engine.publisher.impl;

import com.google.common.base.Joiner;
import com.typesafe.config.Config;
import javafx.scene.control.Alert;
import org.apache.commons.lang3.StringUtils;
import org.apache.eagle.alert.engine.codec.IEventSerializer;
import org.apache.eagle.alert.engine.coordinator.OverrideDeduplicatorSpec;
import org.apache.eagle.alert.engine.coordinator.Publishment;
import org.apache.eagle.alert.engine.model.AlertPublishEvent;
import org.apache.eagle.alert.engine.model.AlertStreamEvent;
import org.apache.eagle.alert.engine.publisher.AlertDeduplicator;
import org.apache.eagle.alert.engine.publisher.AlertPublishPlugin;
import org.apache.eagle.alert.engine.publisher.dedup.DedupCache;
import org.apache.eagle.alert.engine.publisher.dedup.ExtendedDeduplicator;
import org.slf4j.Logger;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @since Jun 3, 2016.
 */
public abstract class AbstractPublishPlugin implements AlertPublishPlugin {

    protected AlertDeduplicator deduplicator;
    protected PublishStatus status;
    protected IEventSerializer serializer;
    protected String pubName;

    @SuppressWarnings("rawtypes")
    @Override
    public void init(Config config, Publishment publishment, Map conf) throws Exception {
        DedupCache dedupCache = new DedupCache(config, publishment.getName());
        OverrideDeduplicatorSpec spec = publishment.getOverrideDeduplicator();
        if (spec != null && StringUtils.isNotBlank(spec.getClassName())) {
            try {
                this.deduplicator = (ExtendedDeduplicator) Class.forName(
                    spec.getClassName()).getConstructor(
                    Config.class,
                    Map.class,
                    List.class,
                    String.class,
                    DedupCache.class,
                    String.class).newInstance(
                    config,
                    spec.getProperties(),
                    publishment.getDedupFields(),
                    publishment.getDedupStateField(),
                    dedupCache,
                    publishment.getName());
                getLogger().info("{} initiliazed extended deduplicator {} with properties {} successfully",
                    publishment.getName(), spec.getClassName(), Joiner.on(",").withKeyValueSeparator(">").join(
                        spec.getProperties() == null ? new HashMap<String, String>() : spec.getProperties()));
            } catch (Throwable t) {
                getLogger().error(String.format("initialize extended deduplicator %s failed", spec.getClassName()), t);
            }
        } else {
            this.deduplicator = new DefaultDeduplicator(publishment.getDedupIntervalMin(),
                publishment.getDedupFields(), publishment.getDedupStateField(), publishment.getDedupStateCloseValue(), dedupCache);
            this.pubName = publishment.getName();
        }
        String serializerClz = publishment.getSerializer();
        try {
            Object obj = Class.forName(serializerClz).getConstructor(Map.class).newInstance(conf);
            if (!(obj instanceof IEventSerializer)) {
                throw new Exception(String.format("serializer %s of publishment %s is not subclass to %s!",
                    publishment.getSerializer(),
                    publishment.getName(),
                    IEventSerializer.class.getName()));
            }
            serializer = (IEventSerializer) obj;
        } catch (Exception e) {
            getLogger().error(String.format("initialized failed, use default StringEventSerializer, failure message : {}", e.getMessage()), e);
            serializer = new StringEventSerializer(conf);
        }
    }

    @Override
    public void update(String dedupIntervalMin, Map<String, Object> pluginProperties) {
        deduplicator.setDedupIntervalMin(dedupIntervalMin);
    }

    @Override
    public List<AlertStreamEvent> dedup(AlertStreamEvent event) {
        return deduplicator.dedup(event);
    }

    @Override
    public PublishStatus getStatus() {
        return status;
    }

    protected abstract Logger getLogger();

}
