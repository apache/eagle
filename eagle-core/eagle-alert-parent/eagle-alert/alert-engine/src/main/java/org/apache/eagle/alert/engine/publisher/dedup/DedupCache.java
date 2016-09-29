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
package org.apache.eagle.alert.engine.publisher.dedup;

import com.google.common.base.Joiner;
import com.google.common.base.Objects;
import com.typesafe.config.Config;
import org.apache.commons.lang.time.DateUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.eagle.alert.engine.coordinator.StreamDefinition;
import org.apache.eagle.alert.engine.model.AlertStreamEvent;
import org.apache.eagle.alert.engine.publisher.dedup.DedupEventsStoreFactory.DedupEventsStoreType;
import org.apache.eagle.alert.engine.publisher.impl.EventUniq;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

public class DedupCache {

    private static final Logger LOG = LoggerFactory.getLogger(DedupCache.class);

    private static final long CACHE_MAX_EXPIRE_TIME_IN_DAYS = 30;
    private static final long CACHE_MAX_EVENT_QUEUE_SIZE = 10;

    public static final String DEDUP_COUNT = "dedupCount";
    public static final String DEDUP_FIRST_OCCURRENCE = "dedupFirstOccurrenceTime";

    private static final DedupEventsStoreType type = DedupEventsStoreType.Mongo;

    private long lastUpdated = -1;
    private Map<EventUniq, ConcurrentLinkedDeque<DedupValue>> events = new ConcurrentHashMap<EventUniq, ConcurrentLinkedDeque<DedupValue>>();

    private static final ConcurrentLinkedDeque<DedupCache> caches = new ConcurrentLinkedDeque<DedupCache>();

    private Config config;

    private String publishName;

    public DedupCache(Config config, String publishName) {
        this.config = config;
        this.publishName = publishName;
        // only happens during startup, won't introduce perf issue here
        synchronized (caches) {
            if (caches.size() == 0) {
                // create daemon to clean up old removable events periodically
                ScheduledExecutorService scheduleSrv = Executors.newScheduledThreadPool(1, new ThreadFactory() {
                    @Override
                    public Thread newThread(Runnable r) {
                        Thread t = new Thread(r);
                        t.setDaemon(true);
                        return t;
                    }
                });
                scheduleSrv.scheduleAtFixedRate(new Runnable() {
                    @Override
                    public void run() {
                        for (DedupCache cache : caches) {
                            if (cache == null || cache.getEvents() == null) {
                                continue;
                            }
                            HashSet<EventUniq> eventUniqs = new HashSet<EventUniq>(cache.getEvents().keySet());
                            for (EventUniq one : eventUniqs) {
                                if (one.removable && one.createdTime < System.currentTimeMillis() - 3600000 * 24) {
                                    cache.removeEvent(one);
                                    LOG.info("Remove dedup key {} from cache & db", one);
                                }
                            }
                        }
                    }
                }, 5, 60, TimeUnit.MINUTES);
                LOG.info("Create daemon to clean up old removable events periodically");
            }
            caches.add(this);
        }
    }

    public Map<EventUniq, ConcurrentLinkedDeque<DedupValue>> getEvents() {
        if (lastUpdated < 0
            || System.currentTimeMillis() - lastUpdated > CACHE_MAX_EXPIRE_TIME_IN_DAYS * DateUtils.MILLIS_PER_DAY
            || events.size() <= 0) {
            lastUpdated = System.currentTimeMillis();
            DedupEventsStore accessor = DedupEventsStoreFactory.getStore(type, this.config, this.publishName);
            events = accessor.getEvents();
        }
        return events;
    }

    public boolean contains(EventUniq eventEniq) {
        return this.getEvents().containsKey(eventEniq);
    }

    public void removeEvent(EventUniq eventEniq) {
        if (this.contains(eventEniq)) {
            this.events.remove(eventEniq);

            DedupEventsStore accessor = DedupEventsStoreFactory.getStore(type, this.config, this.publishName);
            accessor.remove(eventEniq);
        }
    }

    public List<AlertStreamEvent> dedup(AlertStreamEvent event, EventUniq eventEniq,
                                        String dedupStateField, String stateFieldValue) {
        DedupValue[] dedupValues = this.addOrUpdate(eventEniq, stateFieldValue);
        if (dedupValues != null) {
            // any of dedupValues won't be null
            if (dedupValues.length == 2) {
                // emit last event which includes count of dedup events & new state event
                return Arrays.asList(
                    this.mergeEventWithDedupValue(event, dedupValues[0], dedupStateField),
                    this.mergeEventWithDedupValue(event, dedupValues[1], dedupStateField));
            } else if (dedupValues.length == 1) {
                //populate firstOccurrenceTime & count
                return Arrays.asList(this.mergeEventWithDedupValue(event, dedupValues[0], dedupStateField));
            }
        }
        // duplicated, will be ignored
        return null;
    }

    public synchronized DedupValue[] addOrUpdate(EventUniq eventEniq, String stateFieldValue) {
        Map<EventUniq, ConcurrentLinkedDeque<DedupValue>> events = this.getEvents();
        if (!events.containsKey(eventEniq)
            || (events.containsKey(eventEniq)
            && events.get(eventEniq).size() > 0
            && !StringUtils.equalsIgnoreCase(stateFieldValue,
            events.get(eventEniq).getLast().getStateFieldValue()))) {
            DedupValue[] dedupValues = this.add(eventEniq, stateFieldValue);
            return dedupValues;
        } else {
            // update count
            this.updateCount(eventEniq);
            return null;
        }
    }

    private DedupValue[] add(EventUniq eventEniq, String stateFieldValue) {
        DedupValue dedupValue = null;
        DedupValue lastDedupValue = null;
        if (!events.containsKey(eventEniq)) {
            dedupValue = new DedupValue();
            dedupValue.setFirstOccurrence(eventEniq.timestamp);
            dedupValue.setStateFieldValue(stateFieldValue);
            ConcurrentLinkedDeque<DedupValue> dedupValues = new ConcurrentLinkedDeque<DedupValue>();
            dedupValues.add(dedupValue);
            // skip the event which put failed due to concurrency
            events.put(eventEniq, dedupValues);
            LOG.info("{} Add new dedup key {}, and value {}", this.publishName, eventEniq, dedupValues);
        } else if (!StringUtils.equalsIgnoreCase(stateFieldValue,
            events.get(eventEniq).getLast().getStateFieldValue())) {
            lastDedupValue = events.get(eventEniq).getLast();
            dedupValue = new DedupValue();
            dedupValue.setFirstOccurrence(eventEniq.timestamp);
            dedupValue.setStateFieldValue(stateFieldValue);
            ConcurrentLinkedDeque<DedupValue> dedupValues = events.get(eventEniq);
            if (dedupValues.size() > CACHE_MAX_EVENT_QUEUE_SIZE) {
                dedupValues = new ConcurrentLinkedDeque<DedupValue>();
                dedupValues.add(lastDedupValue);
                LOG.info("{} Reset dedup key {} to value {} since meets maximum {}",
                    this.publishName, eventEniq, dedupValue, CACHE_MAX_EVENT_QUEUE_SIZE);
            }
            dedupValues.add(dedupValue);
            LOG.info("{} Update dedup key {}, and value {}", this.publishName, eventEniq, dedupValue);
        }
        if (dedupValue != null) {
            DedupEventsStore accessor = DedupEventsStoreFactory.getStore(type, this.config, this.publishName);
            accessor.add(eventEniq, events.get(eventEniq));
            LOG.info("{} Store dedup key {}, value {} to DB", this.publishName, eventEniq,
                Joiner.on(",").join(events.get(eventEniq)));
        }
        if (dedupValue == null) {
            return null;
        }
        if (lastDedupValue != null) {
            return new DedupValue[] {lastDedupValue, dedupValue};
        } else {
            return new DedupValue[] {dedupValue};
        }
    }

    public void persistUpdatedEventUniq(EventUniq eventEniq) {
        DedupEventsStore accessor = DedupEventsStoreFactory.getStore(type, this.config, this.publishName);
        accessor.add(eventEniq, events.get(eventEniq));
        LOG.info("{} Store dedup key {}, value {} to DB", this.publishName, eventEniq,
            Joiner.on(",").join(events.get(eventEniq)));
    }

    private DedupValue updateCount(EventUniq eventEniq) {
        ConcurrentLinkedDeque<DedupValue> dedupValues = events.get(eventEniq);
        if (dedupValues == null || dedupValues.size() <= 0) {
            LOG.warn("{} No dedup values found for {}, cannot update count", this.publishName, eventEniq);
            return null;
        } else {
            DedupValue dedupValue = dedupValues.getLast();
            dedupValue.setCount(dedupValue.getCount() + 1);
            String updateMsg = String.format(
                "{} Update count for dedup key %s, value %s and count %s", this.publishName, eventEniq,
                dedupValue.getStateFieldValue(), dedupValue.getCount());
            if (dedupValue.getCount() > 0 && dedupValue.getCount() % 100 == 0) {
                LOG.info(updateMsg);
                DedupEventsStore accessor = DedupEventsStoreFactory.getStore(type, this.config, this.publishName);
                accessor.add(eventEniq, dedupValues);
            } else if (LOG.isDebugEnabled()) {
                LOG.debug(updateMsg);
            }
            return dedupValue;
        }
    }

    private AlertStreamEvent mergeEventWithDedupValue(AlertStreamEvent originalEvent,
                                                      DedupValue dedupValue, String dedupStateField) {
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

}
