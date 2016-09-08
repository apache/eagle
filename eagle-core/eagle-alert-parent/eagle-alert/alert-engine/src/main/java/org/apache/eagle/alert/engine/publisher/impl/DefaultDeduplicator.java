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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedDeque;

import org.apache.commons.lang3.StringUtils;
import org.apache.eagle.alert.engine.coordinator.StreamDefinition;
import org.apache.eagle.alert.engine.model.AlertStreamEvent;
import org.apache.eagle.alert.engine.publisher.AlertDeduplicator;
import org.apache.eagle.alert.engine.publisher.dedup.DedupCache;
import org.apache.eagle.alert.engine.publisher.dedup.DedupValue;
import org.apache.storm.guava.base.Objects;
import org.joda.time.Period;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.typesafe.config.Config;

public class DefaultDeduplicator implements AlertDeduplicator {

    private static Logger LOG = LoggerFactory.getLogger(DefaultDeduplicator.class);

    @SuppressWarnings("unused")
    private long dedupIntervalMin;
    private List<String> customDedupFields = new ArrayList<>();
    private String dedupStateField;
    private String dedupStateCloseValue;
    private Config config;

    private static final String DEDUP_COUNT = "dedupCount";
    private static final String DEDUP_FIRST_OCCURRENCE = "dedupFirstOccurrence";

    private DedupCache dedupCache;

    public DefaultDeduplicator() {
        this.dedupIntervalMin = 0;
    }

    public DefaultDeduplicator(String intervalMin) {
        setDedupIntervalMin(intervalMin);
    }

    public DefaultDeduplicator(long intervalMin) {
        this.dedupIntervalMin = intervalMin;
    }

    public DefaultDeduplicator(String intervalMin, List<String> customDedupFields,
                               String dedupStateField, String dedupStateCloseValue, Config config) {
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
        this.config = config;
        this.dedupCache = DedupCache.getInstance(this.config);
    }

    /*
     * @param key
     * @return
     */
    public List<AlertStreamEvent> checkDedup(AlertStreamEvent event, EventUniq key, String stateFiledValue) {
        if (StringUtils.isBlank(stateFiledValue)) {
            // without state field, we cannot determine whether it is duplicated
            return Arrays.asList(event);
        }
        synchronized (dedupCache) {
            Map<EventUniq, ConcurrentLinkedDeque<DedupValue>> events = dedupCache.getEvents();
            if (!events.containsKey(key)
                || (events.containsKey(key)
                && events.get(key).size() > 0
                && !Objects.equal(stateFiledValue,
                        events.get(key).getLast().getStateFieldValue()))) {
                DedupValue[] dedupValues = dedupCache.add(key, stateFiledValue, dedupStateCloseValue);
                if (dedupValues != null) {
                    // any of dedupValues won't be null
                    if (dedupValues.length == 2) {
                        // emit last event which includes count of dedup events & new state event
                        return Arrays.asList(
                            mergeEventWithDedupValue(event, dedupValues[0]),
                            mergeEventWithDedupValue(event, dedupValues[1]));
                    } else if (dedupValues.length == 1) {
                        //populate firstOccurrenceTime & count
                        return Arrays.asList(mergeEventWithDedupValue(event, dedupValues[0]));
                    }
                }
            } else {
                // update count
                dedupCache.updateCount(key);
            }
        }
        // duplicated, will be ignored
        return null;
    }

    private AlertStreamEvent mergeEventWithDedupValue(AlertStreamEvent originalEvent, DedupValue dedupValue) {
        AlertStreamEvent event = new AlertStreamEvent();
        Object[] newdata = new Object[originalEvent.getData().length];
        for (int i = 0; i < originalEvent.getData().length; i++) {
            newdata[i] = originalEvent.getData()[i];
        }
        event.setData(newdata);
        event.setSchema(originalEvent.getSchema());
        event.setPolicyId(originalEvent.getPolicyId());
        event.setCreatedTime(originalEvent.getCreatedTime());
        event.setCreatedBy(originalEvent.getCreatedBy());
        event.setTimestamp(originalEvent.getTimestamp());
        StreamDefinition streamDefinition = event.getSchema();
        for (int i = 0; i < event.getData().length; i++) {
            String colName = streamDefinition.getColumns().get(i).getName();
            if (Objects.equal(colName, dedupStateField)) {
                event.getData()[i] = dedupValue.getStateFieldValue();
            }
            if (Objects.equal(colName, DEDUP_COUNT)) {
                event.getData()[i] = dedupValue.getCount();
            }
            if (Objects.equal(colName, DEDUP_FIRST_OCCURRENCE)) {
                event.getData()[i] = dedupValue.getFirstOccurrence();
            }
        }
        return event;
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

            for (String field : customDedupFields) {
                if (colName.equals(field)) {
                    customFieldValues.put(field, event.getData()[i].toString());
                    break;
                }
            }
        }

        List<AlertStreamEvent> outputEvents = checkDedup(event, new EventUniq(event.getStreamId(),
            event.getPolicyId(), event.getCreatedTime(), customFieldValues), stateFiledValue);
        if (outputEvents != null && outputEvents.size() > 0) {
            return outputEvents;
        } else if (LOG.isDebugEnabled()) {
            LOG.debug("Alert event is skipped because it's duplicated: {}", event.toString());
        }
        return null;
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
