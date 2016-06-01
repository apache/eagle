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
package org.apache.eagle.alert.dedup;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang.time.DateUtils;
import org.apache.eagle.alert.entity.AlertAPIEntity;
import org.apache.eagle.common.metric.AlertContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultDeduplicator implements EntityDeduplicator {
	protected long dedupIntervalMin;
	protected List<String> fields;
	protected Map<EntityDedupKey, Long> entites = new HashMap<>();
	public static Logger LOG = LoggerFactory.getLogger(DefaultDeduplicator.class);
	
	public static enum AlertDeduplicationStatus{
		NEW,
		DUPLICATED,
		IGNORED
	}
	
	public DefaultDeduplicator() {
		this.dedupIntervalMin = 0;
		fields = null;
	}

	public DefaultDeduplicator(long intervalMin, List<String> fields) {
		this.dedupIntervalMin = intervalMin;
		this.fields = fields;
	}
	
	public void clearOldCache() {
		List<EntityDedupKey> removedkeys = new ArrayList<>();
		for (Entry<EntityDedupKey, Long> entry : entites.entrySet()) {
			EntityDedupKey entity = entry.getKey();
			if (System.currentTimeMillis() - 7 * DateUtils.MILLIS_PER_DAY > entity.createdTime) {
				removedkeys.add(entry.getKey());
			}
		}
		for (EntityDedupKey alertKey : removedkeys) {
			entites.remove(alertKey);
		}
	}
	
	public AlertDeduplicationStatus checkDedup(EntityDedupKey key){
		long current = key.timestamp;
		if(!entites.containsKey(key)){
			entites.put(key, current);
			return AlertDeduplicationStatus.NEW;
		}
		
		long last = entites.get(key);
		if(current - last >= dedupIntervalMin * DateUtils.MILLIS_PER_MINUTE){
			entites.put(key, current);
			return AlertDeduplicationStatus.DUPLICATED;
		}
		
		return AlertDeduplicationStatus.IGNORED;
	}

	private List<String> getKeyList(AlertAPIEntity entity) {
		List<String> keys = new ArrayList<>(entity.getTags().values());
		if(fields != null && !fields.isEmpty()) {
			for (String field: fields) {
				AlertContext context = entity.getWrappedAlertContext();
				keys.add(context.getProperty(field));
			}
		}
		return keys;
	}

	public List<AlertAPIEntity> dedup(List<AlertAPIEntity> list) {
		clearOldCache();
		List<AlertAPIEntity> dedupList = new ArrayList<>();
        int totalCount = list.size();
        int dedupedCount = 0;
		for(AlertAPIEntity entity: list) {
			if (entity.getTags() == null) {
				if(LOG.isDebugEnabled()) LOG.debug("Tags is null, don't know how to deduplicate, do nothing");
			} else {
                AlertDeduplicationStatus status = checkDedup(new EntityDedupKey(getKeyList(entity), entity.getTimestamp()));
                if (!status.equals(AlertDeduplicationStatus.IGNORED)) {
                    dedupList.add(entity);
                } else {
                    dedupedCount++;
                    if (LOG.isDebugEnabled())
                        LOG.debug(String.format("Entity is skipped because it's duplicated: " + entity.toString()));
                }
            }
		}

        if(dedupedCount>0){
            LOG.info(String.format("Skipped %s of %s alerts because they are duplicated", dedupedCount, totalCount));
        }else if(LOG.isDebugEnabled()){
            LOG.debug(String.format("Skipped %s of %s duplicated alerts",dedupedCount,totalCount));
        }

		return dedupList;
	}

	public EntityDeduplicator setDedupIntervalMin(long dedupIntervalMin) {
		this.dedupIntervalMin = dedupIntervalMin;
		return this;
	}
	
	public long getDedupIntervalMin() {
		return dedupIntervalMin;
	}
}
