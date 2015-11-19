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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.eagle.log.base.taggedlog.TaggedLogAPIEntity;

public class DefaultDeduplicator<T extends TaggedLogAPIEntity> implements EntityDeduplicator<T>{
	protected long dedupIntervalMin;
	protected Map<EntityTagsUniq, Long> entites = new HashMap<EntityTagsUniq, Long>();
	public static Logger LOG = LoggerFactory.getLogger(DefaultDeduplicator.class);
	
	public static enum AlertDeduplicationStatus{
		NEW,
		DUPLICATED,
		IGNORED
	}
	
	public DefaultDeduplicator() {
		this.dedupIntervalMin = 0;
	}
	
	public DefaultDeduplicator(long intervalMin) {
		this.dedupIntervalMin = intervalMin;
	}
	
	public void clearOldCache() {
		List<EntityTagsUniq> removedkeys = new ArrayList<EntityTagsUniq>();
		for (Entry<EntityTagsUniq, Long> entry : entites.entrySet()) {
			EntityTagsUniq entity = entry.getKey();
			if (System.currentTimeMillis() - 7 * DateUtils.MILLIS_PER_DAY > entity.createdTime) {
				removedkeys.add(entry.getKey());
			}
		}
		for (EntityTagsUniq alertKey : removedkeys) {
			entites.remove(alertKey);
		}
	}
	
	public AlertDeduplicationStatus checkDedup(EntityTagsUniq key){
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
	
	public List<T> dedup(List<T> list) {
		clearOldCache();
		List<T> dedupList = new ArrayList<T>();
        int totalCount = list.size();
        int dedupedCount = 0;
		for(T entity: list) {
			if (entity.getTags() == null) {
				if(LOG.isDebugEnabled()) LOG.debug("Tags is null, don't know how to deduplicate, do nothing");
			} else {
                AlertDeduplicationStatus status = checkDedup(new EntityTagsUniq(entity.getTags(), entity.getTimestamp()));
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
            LOG.info(String.format("Skipped %s of %s alerts because they are duplicated",dedupedCount,totalCount));
        }else if(LOG.isDebugEnabled()){
            LOG.debug(String.format("Skipped %s of %s duplicated alerts",dedupedCount,totalCount));
        }

		return dedupList;
	}

	public EntityDeduplicator<T> setDedupIntervalMin(long dedupIntervalMin) {
		this.dedupIntervalMin = dedupIntervalMin;
		return this;
	}
	
	public long getDedupIntervalMin() {
		return dedupIntervalMin;
	}
}
