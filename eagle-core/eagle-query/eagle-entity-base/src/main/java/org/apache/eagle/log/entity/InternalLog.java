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
package org.apache.eagle.log.entity;

import org.apache.eagle.common.DateTimeUtil;

import java.util.List;
import java.util.Map;

/**
 * TODO we should decouple BaseLog during write time and BaseLog during read time
 */
public class InternalLog {
	private String encodedRowkey;
	private String prefix;
	private String[] partitions;
	private long timestamp;
	private Map<String, byte[]> qualifierValues;

	private Map<String,Object> extraValues;
	private Map<String, String> tags;
	private Map<String, List<String>> searchTags;
	private List<byte[]> indexRowkeys;

	public String getEncodedRowkey() {
		return encodedRowkey;
	}

	public void setEncodedRowkey(String encodedRowkey) {
		this.encodedRowkey = encodedRowkey;
	}
	
	public Map<String, byte[]> getQualifierValues() {
		return qualifierValues;
	}
	public void setQualifierValues(Map<String, byte[]> qualifierValues) {
		this.qualifierValues = qualifierValues;
	}

	public Map<String, List<String>> getSearchTags() {
		return searchTags;
	}
	public void setSearchTags(Map<String, List<String>> searchTags) {
		this.searchTags = searchTags;
	}
	public String getPrefix() {
		return prefix;
	}
	public void setPrefix(String prefix) {
		this.prefix = prefix;
	}
	public String[] getPartitions() {
		return partitions;
	}
	public void setPartitions(String[] partitions) {
		this.partitions = partitions;
	}
	public long getTimestamp() {
		return timestamp;
	}
	public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}
	public Map<String, String> getTags() {
		return tags;
	}
	public void setTags(Map<String, String> tags) {
		this.tags = tags;
	}
	public List<byte[]> getIndexRowkeys() {
		return indexRowkeys;
	}
	public void setIndexRowkeys(List<byte[]> indexRowkeys) {
		this.indexRowkeys = indexRowkeys;
	}
	public Map<String, Object> getExtraValues() { return extraValues; }
	public void setExtraValues(Map<String, Object> extraValues) { this.extraValues = extraValues; }

	public String toString(){
		StringBuffer sb = new StringBuffer();
		sb.append(prefix);
		sb.append("|");
		sb.append(DateTimeUtil.millisecondsToHumanDateWithMilliseconds(timestamp));
		sb.append("(");
		sb.append(timestamp);
		sb.append(")");
		sb.append("|searchTags:");
		if(searchTags != null){
			for(String tagkey : searchTags.keySet()){
				sb.append(tagkey);
				sb.append('=');
				List<String> tagValues = searchTags.get(tagkey);
				sb.append("(");
				for(String tagValue : tagValues){
					sb.append(tagValue);
					sb.append(",");
				}
				sb.append(")");
				sb.append(",");
			}
		}
		sb.append("|tags:");
		if(tags != null){
			for(Map.Entry<String, String> entry : tags.entrySet()){
				sb.append(entry.getKey());
				sb.append("=");
				sb.append(entry.getValue());
				sb.append(",");
			}
		}
		sb.append("|columns:");
		if(qualifierValues != null){
			for(String qualifier : qualifierValues.keySet()){
				byte[] value = qualifierValues.get(qualifier);
				sb.append(qualifier);
				sb.append("=");
				if(value != null){
					sb.append(new String(value));
				}
				sb.append(",");
			}
		}
		return sb.toString();
	}
}
