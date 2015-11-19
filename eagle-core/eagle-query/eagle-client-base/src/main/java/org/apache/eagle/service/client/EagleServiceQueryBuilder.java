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

import java.util.ArrayList;
import java.util.List;

/**
 * Builder helper class to create EagleServiceQueryRequest
 */
public final class EagleServiceQueryBuilder {

	private final EagleServiceQueryRequest request = new EagleServiceQueryRequest();
	
	public EagleServiceQueryBuilder addSearchTag(String tagKey, String tagValue) throws EagleServiceClientException {
		if (tagKey == null || tagValue == null) {
			throw new EagleServiceClientException("tagKey or tagValue is null, tagKey: " + tagKey + ", tagValue: " + tagValue);
		}
		List<EagleServiceQueryRequest.Tag> searchTags = request.getSearchTags();
		if (searchTags == null) {
			searchTags = new ArrayList<EagleServiceQueryRequest.Tag>();
			request.setSearchTags(searchTags);
		}
		if (!containsTag(tagKey, tagValue)) {
			final EagleServiceQueryRequest.Tag tag = new EagleServiceQueryRequest.Tag();
			tag.setKey(tagKey);
			tag.setValue(tagValue);
			searchTags.add(tag);
		}
		return this;
	}
	
	public EagleServiceQueryRequest buildRequest() throws EagleServiceClientException {
		return request;
	}
	
	public EagleServiceQueryBuilder setStartTime(long startTime) {
		request.setStartTime(startTime);
		return this;
	}
	
	public EagleServiceQueryBuilder setEndTime(long endTime) {
		request.setEndTime(endTime);
		return this;
	}
	
	public EagleServiceQueryBuilder setPageSize(int pageSize) throws EagleServiceClientException {
		if (pageSize <= 0) {
			throw new EagleServiceClientException("pageSize can't be less than 1");
		}
		request.setPageSize(pageSize);
		return this;
	}

	public EagleServiceQueryBuilder addReturnTag(String tagKey) throws EagleServiceClientException {
		if (tagKey == null) {
			throw new EagleServiceClientException("tagKey can't be null");
		}
		List<String> returnTags = request.getReturnTags();
		if (returnTags == null) {
			returnTags = new ArrayList<String>();
			request.setReturnTags(returnTags);
		}
		if (!returnTags.contains(tagKey)) {
			returnTags.add(tagKey);
		}
		return this;
	}
	
	public EagleServiceQueryBuilder addReturnField(String field) throws EagleServiceClientException {
		if (field == null) {
			throw new EagleServiceClientException("field can't be null");
		}
		List<String> returnFields = request.getReturnFields();
		if (returnFields == null) {
			returnFields = new ArrayList<String>();
			request.setReturnFields(returnFields);
		}
		if (!returnFields.contains(field)) {
			returnFields.add(field);
		}
		return this;
	}

	private boolean containsTag(String tagKey, String tagValue) {
		for (EagleServiceQueryRequest.Tag tag : request.getSearchTags()) {
			if (tag.getKey().equals(tagKey) && tag.getValue().equals(tagValue)) {
				return true;
			}
		}
		return false;
	}
}
