/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */
package org.apache.eagle.alert.engine.publisher.impl;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.apache.commons.lang3.StringUtils;
import org.apache.eagle.alert.engine.coordinator.StreamDefinition;
import org.apache.eagle.alert.engine.model.AlertStreamEvent;
import org.apache.eagle.alert.engine.publisher.AlertDeduplicator;
import org.apache.eagle.alert.engine.publisher.dedup.DedupCache;
import org.joda.time.Period;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class DefaultDeduplicator implements AlertDeduplicator {

    private static Logger LOG = LoggerFactory.getLogger(DefaultDeduplicator.class);

    private long dedupIntervalSec;
    private List<String> customDedupFields = new ArrayList<>();
    private String dedupStateField;
    private String dedupStateCloseValue;

    private DedupCache dedupCache;

    private Cache<EventUniq, String> withoutStatesCache;

    public DefaultDeduplicator() {
        this.dedupIntervalSec = 0;
    }

    public DefaultDeduplicator(String intervalMin) {
        setDedupIntervalMin(intervalMin);
    }

    public DefaultDeduplicator(long intervalMin) {
        this.dedupIntervalSec = intervalMin;
    }

    public DefaultDeduplicator(String intervalMin, List<String> customDedupFields,
                               String dedupStateField, String dedupStateCloseValue, DedupCache dedupCache) {
        setDedupIntervalMin(intervalMin);
        if (customDedupFields != null) {
            this.customDedupFields = customDedupFields;
        }
        if (StringUtils.isNotBlank(dedupStateField)) {
            this.dedupStateField = dedupStateField;
        }
        if (StringUtils.isNotBlank(dedupStateCloseValue)) {
            this.dedupStateCloseValue = dedupStateCloseValue;
        }
        this.dedupCache = dedupCache;

        withoutStatesCache = CacheBuilder.newBuilder().expireAfterWrite(
            this.dedupIntervalSec, TimeUnit.SECONDS).build();
    }

    /*
     * @param key
     * @return
     */
    public List<AlertStreamEvent> checkDedup(AlertStreamEvent event, EventUniq key, String stateFiledValue) {
        if (StringUtils.isBlank(stateFiledValue)) {
            // without state field, we cannot determine whether it is duplicated
            // without custom filed values, we cannot determine whether it is duplicated
            synchronized (withoutStatesCache) {
                if (withoutStatesCache != null && withoutStatesCache.getIfPresent(key) != null) {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Alert event {} with key {} is skipped since it is duplicated", event, key);
                    }
                    return null;
                } else if (withoutStatesCache != null) {
                    withoutStatesCache.put(key, "");
                }
            }
            return Arrays.asList(event);
        }
        return dedupCache.dedup(event, key, dedupStateField, stateFiledValue, dedupStateCloseValue);
    }

    public List<AlertStreamEvent> dedup(AlertStreamEvent event) {
        if (event == null) {
            return null;
        }
        // check custom field, and get the field values
        StreamDefinition streamDefinition = event.getSchema();
        HashMap<String, String> customFieldValues = new HashMap<>();
        String stateFiledValue = null;
        for (int i = 0; i < event.getData().length; i++) {
            if (i > streamDefinition.getColumns().size()) {
                if (LOG.isWarnEnabled()) {
                    LOG.warn("output column does not found for event data, this indicate code error!");
                }
                continue;
            }
            String colName = streamDefinition.getColumns().get(i).getName();

            if (colName.equals(dedupStateField)) {
                stateFiledValue = event.getData()[i].toString();
            }

            // make all of the field as unique key if no custom dedup field provided
            if (customDedupFields == null || customDedupFields.size() <= 0) {
                customFieldValues.put(colName, event.getData()[i].toString());
            } else {
                for (String field : customDedupFields) {
                    if (colName.equals(field)) {
                        customFieldValues.put(field, event.getData()[i].toString());
                        break;
                    }
                }
            }
        }

        List<AlertStreamEvent> outputEvents = checkDedup(event, new EventUniq(event.getStreamId(),
            event.getPolicyId(), event.getCreatedTime(), customFieldValues), stateFiledValue);
        if (outputEvents != null && outputEvents.size() > 0) {
            return outputEvents;
        } else if (LOG.isInfoEnabled()) {
            LOG.info("Alert event is skipped because it's duplicated: {}", event.toString());
        }
        return null;
    }

    @Override
    public void setDedupIntervalMin(String newDedupIntervalMin) {
        if (newDedupIntervalMin == null || newDedupIntervalMin.isEmpty()) {
            dedupIntervalSec = 0;
            return;
        }
        try {
            Period period = Period.parse(newDedupIntervalMin);
            this.dedupIntervalSec = period.toStandardSeconds().getSeconds();
        } catch (Exception e) {
            LOG.warn("Fail to pares deDupIntervalMin, will disable deduplication instead", e);
            this.dedupIntervalSec = 0;
        }
    }

}
