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

import org.apache.commons.lang.time.DateUtils;
import org.apache.eagle.alert.engine.coordinator.StreamDefinition;
import org.apache.eagle.alert.engine.model.AlertStreamEvent;
import org.apache.eagle.alert.engine.publisher.AlertDeduplicator;
import org.joda.time.Period;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

public class DefaultDeduplicator implements AlertDeduplicator {
    private long dedupIntervalMin;
    private List<String> customDedupFields = new ArrayList<>();
    private volatile Map<EventUniq, Long> events = new HashMap<>();
    private static Logger LOG = LoggerFactory.getLogger(DefaultDeduplicator.class);

    public enum AlertDeduplicationStatus {
        NEW,
        DUPLICATED,
        IGNORED
    }

    public DefaultDeduplicator() {
        this.dedupIntervalMin = 0;
    }

    public DefaultDeduplicator(String intervalMin) {
        setDedupIntervalMin(intervalMin);
    }

    public DefaultDeduplicator(long intervalMin) {
        this.dedupIntervalMin = intervalMin;
    }

    public DefaultDeduplicator(String intervalMin, List<String> customDedupFields) {
        setDedupIntervalMin(intervalMin);
        if (customDedupFields != null) {
            this.customDedupFields = customDedupFields;
        }
    }

    public void clearOldCache() {
        List<EventUniq> removedkeys = new ArrayList<>();
        for (Entry<EventUniq, Long> entry : events.entrySet()) {
            EventUniq entity = entry.getKey();
            if (System.currentTimeMillis() - 7 * DateUtils.MILLIS_PER_DAY > entity.createdTime) {
                removedkeys.add(entry.getKey());
            }
        }
        for (EventUniq alertKey : removedkeys) {
            events.remove(alertKey);
        }
    }

    /***
     *
     * @param key
     * @return
     */
    public AlertDeduplicationStatus checkDedup(EventUniq key) {
        long current = key.timestamp;
        if (!events.containsKey(key)) {
            events.put(key, current);
            return AlertDeduplicationStatus.NEW;
        }

        long last = events.get(key);
        if (current - last >= dedupIntervalMin * DateUtils.MILLIS_PER_MINUTE) {
            events.put(key, current);
            return AlertDeduplicationStatus.IGNORED;
        }

        return AlertDeduplicationStatus.DUPLICATED;
    }

    public AlertStreamEvent dedup(AlertStreamEvent event) {
        if (event == null) {
            return null;
        }
        clearOldCache();
        AlertStreamEvent result = null;

        // check custom field, and get the field values
        StreamDefinition streamDefinition = event.getSchema();
        HashMap<String, String> customFieldValues = new HashMap<>();
        for (int i = 0; i < event.getData().length; i++) {
            if (i > streamDefinition.getColumns().size()) {
                if (LOG.isWarnEnabled()) {
                    LOG.warn("output column does not found for event data, this indicate code error!");
                }
                continue;
            }
            String colName = streamDefinition.getColumns().get(i).getName();

            for (String field : customDedupFields) {
                if (colName.equals(field)) {
                    customFieldValues.put(field, event.getData()[i].toString());
                    break;
                }
            }
        }

        AlertDeduplicationStatus status = checkDedup(
            new EventUniq(event.getStreamId(),
                event.getPolicyId(),
                event.getCreatedTime(),
                customFieldValues));
        if (!status.equals(AlertDeduplicationStatus.DUPLICATED)) {
            result = event;
        } else if (LOG.isDebugEnabled()) {
            LOG.debug("Alert event is skipped because it's duplicated: {}", event.toString());
        }
        return result;
    }

    @Override
    public void setDedupIntervalMin(String newDedupIntervalMin) {
        if (newDedupIntervalMin == null || newDedupIntervalMin.isEmpty()) {
            dedupIntervalMin = 0;
            return;
        }
        try {
            Period period = Period.parse(newDedupIntervalMin);
            this.dedupIntervalMin = period.toStandardMinutes().getMinutes();
        } catch (Exception e) {
            LOG.warn("Fail to pares deDupIntervalMin, will disable deduplication instead", e);
            this.dedupIntervalMin = 0;
        }
    }

}
