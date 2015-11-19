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
package org.apache.eagle.log.entity;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;


/**
 * @since Sep 12, 2014
 */
public class EntityUniq {
	
	public Map<String, String> tags;
	public Long timestamp;
	public long createdTime; // for cache removal;
	
	public EntityUniq(Map<String, String> tags, long timestamp) {
		this.tags = new HashMap<String, String>(tags);
		this.timestamp = timestamp;
		this.createdTime = System.currentTimeMillis();
	}
	
	@Override	
	public boolean equals(Object obj) {		
		if (obj instanceof EntityUniq) {
			EntityUniq au = (EntityUniq) obj;
			if (tags.size() != au.tags.size()) return false;
			for (Entry<String, String> keyValue : au.tags.entrySet()) {
				boolean keyExist = tags.containsKey(keyValue.getKey());
				if ( !keyExist || !tags.get(keyValue.getKey()).equals(keyValue.getValue())) {				
					return false;
				}
			}
			if (!timestamp.equals(au.timestamp)) return false;
			return true;
		}
		return false;
	}
	
	@Override
	public int hashCode() {	
		int hashCode = 0;
		for (String value : tags.values()) {
			hashCode ^= value.hashCode();	
		}
		return hashCode ^= timestamp.hashCode();
	}
}
