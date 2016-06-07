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
/**
 * 
 */
package org.apache.eagle.alert.dedup;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @since Mar 19, 2015
 */
public class EntityTagsUniq {
	public Map<String, String> tags;
	public Long timestamp;	 // entity's timestamp
	public long createdTime; // entityTagsUniq's created time, for cache removal;
	
	private static final Logger LOG = LoggerFactory.getLogger(EntityTagsUniq.class);
	
	public EntityTagsUniq(Map<String, String> tags, long timestamp) {
		this.tags = new HashMap<String, String>(tags);
		this.timestamp = timestamp;
		this.createdTime = System.currentTimeMillis();
	}
	
	@Override	
	public boolean equals(Object obj) {		
		if (obj instanceof EntityTagsUniq) {
			EntityTagsUniq au = (EntityTagsUniq) obj;
			if (tags.size() != au.tags.size()) return false;
			for (Entry<String, String> keyValue : au.tags.entrySet()) {
				boolean keyExist = tags.containsKey(keyValue.getKey());
				// sanity check
				if (tags.get(keyValue.getKey()) == null || keyValue.getValue() == null) {
					return true;
				}
				if ( !keyExist || !tags.get(keyValue.getKey()).equals(keyValue.getValue())) {				
					return false;
				}
			}
			return true; 
		}
		return false;
	}
	
	@Override
	public int hashCode() {	
		int hashCode = 0;
		for (Map.Entry<String,String> entry : tags.entrySet()) {
            if(entry.getValue() == null) {
                LOG.warn("Tag value for key ["+entry.getKey()+"] is null, skipped for hash code");
            }else {
                try {
                    hashCode ^= entry.getValue().hashCode();
                } catch (Throwable t) {
                    LOG.info("Got exception because of entry: " + entry, t);
                }
            }
		}
		return hashCode;
	}
}