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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;

import org.apache.commons.lang.time.DateUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.eagle.alert.engine.publisher.dedup.DedupEventsStoreFactory.DedupEventsStoreType;
import org.apache.eagle.alert.engine.publisher.impl.EventUniq;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Joiner;
import com.google.common.base.Objects;
import com.typesafe.config.Config;

/**
 * 
 * it is not thread safe, we need to handle concurrency issue out of this class
 *
 */
public class DedupCache {
	
	private static final Logger LOG = LoggerFactory.getLogger(DedupCache.class);

	private final static long CACHE_MAX_EXPIRE_TIME_IN_DAYS = 30;
	private final static long CACHE_MAX_EVENT_QUEUE_SIZE = 10;
	
	private final static DedupEventsStoreType type = DedupEventsStoreType.Mongo;
	
	private long lastUpdated = -1;
	private Map<EventUniq, ConcurrentLinkedDeque<DedupValue>> events = new ConcurrentHashMap<EventUniq, ConcurrentLinkedDeque<DedupValue>>();
	
	private Config config;
	
	private static DedupCache INSTANCE;
	
	public static synchronized DedupCache getInstance(Config config) {
		if (INSTANCE == null) {
			INSTANCE = new DedupCache();
			INSTANCE.config = config;
		}
		return INSTANCE;
	}
	
	public Map<EventUniq, ConcurrentLinkedDeque<DedupValue>> getEvents() {
		if (lastUpdated < 0 || 
				System.currentTimeMillis() - lastUpdated > CACHE_MAX_EXPIRE_TIME_IN_DAYS * DateUtils.MILLIS_PER_DAY || 
				events.size() <= 0) {
			lastUpdated = System.currentTimeMillis();
			DedupEventsStore accessor = DedupEventsStoreFactory.getStore(type, this.config);
			events = accessor.getEvents();
		}
		return events;
	}
	
	public DedupValue[] add(EventUniq eventEniq, String stateFieldValue, String dedupStateCloseValue) {
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
			LOG.info("Add new dedup key {}, and value {}", eventEniq, dedupValues);
		} else if (!Objects.equal(stateFieldValue, 
				events.get(eventEniq).getLast().getStateFieldValue())) {
			lastDedupValue = events.get(eventEniq).getLast();
			dedupValue = new DedupValue();
			dedupValue.setFirstOccurrence(eventEniq.timestamp);
			dedupValue.setStateFieldValue(stateFieldValue);
			ConcurrentLinkedDeque<DedupValue> dedupValues = events.get(eventEniq);
			if (dedupValues.size() > CACHE_MAX_EVENT_QUEUE_SIZE) {
				dedupValues = new ConcurrentLinkedDeque<DedupValue>();
				dedupValues.add(lastDedupValue);
			}
			dedupValues.add(dedupValue);
			LOG.info("Update dedup key {}, and value {}", eventEniq, dedupValue);
		}
		if (dedupValue != null) {
			// reset the list if close state reached
			if (StringUtils.isNotBlank(dedupStateCloseValue) && 
					Objects.equal(stateFieldValue, dedupStateCloseValue)) {
				events.put(eventEniq, new ConcurrentLinkedDeque<DedupValue>());
				events.get(eventEniq).add(dedupValue);
				LOG.info("Reset dedup key {} to value {}", eventEniq, dedupValue);
			}
			
			DedupEventsStore accessor = DedupEventsStoreFactory.getStore(type, this.config);
			accessor.add(eventEniq, events.get(eventEniq));
			LOG.info("Store dedup key {}, value {} to DB", eventEniq, 
					Joiner.on(",").join(events.get(eventEniq)));
		}
		if (dedupValue == null) {
			return null;
		}
		if (lastDedupValue != null) {
			return new DedupValue[] { lastDedupValue, dedupValue };
		} else {
			return new DedupValue[] { dedupValue };
		}
	}
	
	public DedupValue updateCount(EventUniq eventEniq) {
		ConcurrentLinkedDeque<DedupValue> dedupValues = events.get(eventEniq);
		if (dedupValues == null || dedupValues.size() <= 0) {
			LOG.warn("No dedup values found for {}, cannot update count", eventEniq);
			return null;
		} else {
			DedupValue dedupValue = dedupValues.getLast();
			dedupValue.setCount(dedupValue.getCount() + 1);
			LOG.info("Update count for dedup key {}, value {} and count {}", eventEniq, 
					dedupValue.getStateFieldValue(), dedupValue.getCount());
			return dedupValue;
		}
	}
	
}
