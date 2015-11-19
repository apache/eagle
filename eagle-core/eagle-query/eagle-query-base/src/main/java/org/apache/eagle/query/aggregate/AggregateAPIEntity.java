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
package org.apache.eagle.query.aggregate;

import java.util.ArrayList;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.map.annotate.JsonSerialize;

@JsonSerialize(include=JsonSerialize.Inclusion.NON_NULL)
public class AggregateAPIEntity {
	private long numDirectDescendants;
	private long numTotalDescendants;
	private String key;
	private SortedMap<String, AggregateAPIEntity> entityList = new TreeMap<String, AggregateAPIEntity>();
	private List<AggregateAPIEntity> sortedList = new ArrayList<AggregateAPIEntity>();

	public String getKey() {
		return key;
	}
	public void setKey(String key) {
		this.key = key;
	}
	@JsonProperty("sL")
	public List<AggregateAPIEntity> getSortedList() {
		return sortedList;
	}
	public void setSortedList(List<AggregateAPIEntity> sortedList) {
		this.sortedList = sortedList;
	}
	@JsonProperty("eL")
	public SortedMap<String, AggregateAPIEntity> getEntityList() {
		return entityList;
	}
	public void setEntityList(SortedMap<String, AggregateAPIEntity> entityList) {
		this.entityList = entityList;
	}
	@JsonProperty("nDD")
	public long getNumDirectDescendants() {
		return numDirectDescendants;
	}
	public void setNumDirectDescendants(long numDirectDescendants) {
		this.numDirectDescendants = numDirectDescendants;
	}
	@JsonProperty("nTD")
	public long getNumTotalDescendants() {
		return numTotalDescendants;
	}
	public void setNumTotalDescendants(long numTotalDescendants) {
		this.numTotalDescendants = numTotalDescendants;
	}
}