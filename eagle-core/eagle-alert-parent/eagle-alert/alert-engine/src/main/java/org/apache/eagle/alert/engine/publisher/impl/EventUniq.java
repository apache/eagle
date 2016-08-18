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
/**
 * 
 */
package org.apache.eagle.alert.engine.publisher.impl;

import org.apache.commons.lang3.builder.HashCodeBuilder;

import java.util.HashMap;

/**
 * @since Mar 19, 2015
 */
public class EventUniq {
	public String streamId;
	public String policyId;
	public Long timestamp;	 // event's createTimestamp
	public long createdTime; // created time, for cache removal;
	public HashMap<String, String> customFieldValues;

	public EventUniq(String streamId, String policyId, long timestamp) {
		this.streamId = streamId;
		this.timestamp = timestamp;
		this.policyId = policyId;
		this.createdTime = System.currentTimeMillis();
	}

	public EventUniq(String streamId, String policyId, long timestamp, HashMap<String, String> customFieldValues) {
		this.streamId = streamId;
		this.timestamp = timestamp;
		this.policyId = policyId;
		this.createdTime = System.currentTimeMillis();
		this.customFieldValues = customFieldValues;
	}

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof EventUniq) {
			EventUniq au = (EventUniq) obj;
			boolean result = this.streamId.equalsIgnoreCase(au.streamId) & this.policyId.equalsIgnoreCase(au.policyId);
			if (this.customFieldValues != null && au.customFieldValues != null) {
				result = result & this.customFieldValues.equals(au.customFieldValues);
			}
			return result;
		}
		return false;
	}

	@Override
	public int hashCode() {
		HashCodeBuilder builder = new HashCodeBuilder().append(streamId).append(policyId);

		if (customFieldValues != null){
			builder.append(customFieldValues);
		}
		return builder.build();
	}
}