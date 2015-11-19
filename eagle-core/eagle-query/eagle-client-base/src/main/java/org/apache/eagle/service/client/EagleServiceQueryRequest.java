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
package org.apache.eagle.service.client;

import org.apache.eagle.common.config.EagleConfigFactory;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

/**
 * Eagle service query parameter request
 */
public class EagleServiceQueryRequest {

	// instance members
	private long startTime;
	private long endTime;
	private int pageSize;
	private List<Tag> searchTags;
	private List<String> returnTags;
	private List<String> returnFields;

	public static class Tag {
		private String key;
		private String value;
		
		public String getKey() {
			return key;
		}
		public void setKey(String key) {
			this.key = key;
		}
		public String getValue() {
			return value;
		}
		public void setValue(String value) {
			this.value = value;
		}
	}

	public long getStartTime() {
		return startTime;
	}

	public void setStartTime(long startTime) {
		this.startTime = startTime;
	}

	public long getEndTime() {
		return endTime;
	}

	public void setEndTime(long endTime) {
		this.endTime = endTime;
	}

	public int getPageSize() {
		return pageSize;
	}

	public void setPageSize(int pageSize) {
		this.pageSize = pageSize;
	}

	public List<Tag> getSearchTags() {
		return searchTags;
	}

	public void setSearchTags(List<Tag> searchTags) {
		this.searchTags = searchTags;
	}

	public List<String> getReturnTags() {
		return returnTags;
	}

	public void setReturnTags(List<String> returnTags) {
		this.returnTags = returnTags;
	}

	public List<String> getReturnFields() {
		return returnFields;
	}

	public void setReturnFields(List<String> returnFields) {
		this.returnFields = returnFields;
	}

	public String getQueryParameterString() throws EagleServiceClientException {
		if (pageSize <= 0) {
			throw new EagleServiceClientException("pageSize can't be less than 1, pageSize: " + pageSize);
		}
		if (startTime > endTime || (startTime == endTime && startTime != 0)) {
			throw new EagleServiceClientException("Invalid startTime and endTime, startTime: " + startTime + ", endTime: " + endTime);
		}
		int returnSize = 0;
		if (returnTags != null) {
			returnSize += returnTags.size();
		}
		if (returnFields != null) {
			returnSize += returnFields.size();
		}
		if (returnSize == 0) {
			throw new EagleServiceClientException("Invalid request, no return tag or field added");
		}
		final StringBuilder sb = new StringBuilder();
		sb.append("pageSize=").append(this.pageSize);
		if (endTime > 0) {
			final SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd%20HH:mm:ss");
            format.setTimeZone(EagleConfigFactory.load().getTimeZone());
			Date date = new Date(startTime);
			String timeString = format.format(date);
			sb.append("&startTime=").append(timeString);
			date.setTime(endTime);
			timeString = format.format(date);
			sb.append("&endTime=").append(timeString);
		}
		if (searchTags != null) {
			for (Tag tag : searchTags) {
				sb.append("&tagNameValue=").append(tag.getKey()).append("%3D").append(tag.getValue());
			}
		}
		if (returnTags != null) {
			for (String tagKey : returnTags) {
				sb.append("&outputTag=").append(tagKey);
			}
		}
		if (returnFields != null) {
			for (String field : returnFields) {
				sb.append("&outputField=").append(field);
			}
		}
		return sb.toString();
	}
	
}
