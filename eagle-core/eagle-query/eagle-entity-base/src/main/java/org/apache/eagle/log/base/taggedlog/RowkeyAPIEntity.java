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
package org.apache.eagle.log.base.taggedlog;

import java.util.Map;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;

@XmlRootElement
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(propOrder = {"success", "exception", "prefixHashCode", "timestamp", "humanTime", "tagNameHashValueHashMap", "fieldNameValueMap"})
public class RowkeyAPIEntity {
	boolean success;
	String exception;
	int prefixHashCode;
	long timestamp;
	String humanTime;
	Map<Integer, Integer> tagNameHashValueHashMap;
	Map<String, String> fieldNameValueMap;
	
	public boolean isSuccess() {
		return success;
	}
	public void setSuccess(boolean success) {
		this.success = success;
	}
	public String getException() {
		return exception;
	}
	public void setException(String exception) {
		this.exception = exception;
	}
	public String getHumanTime() {
		return humanTime;
	}
	public void setHumanTime(String humanTime) {
		this.humanTime = humanTime;
	}
	public int getPrefixHashCode() {
		return prefixHashCode;
	}
	public void setPrefixHashCode(int prefixHashcode) {
		this.prefixHashCode = prefixHashcode;
	}
	public long getTimestamp() {
		return timestamp;
	}
	public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}
	public Map<Integer, Integer> getTagNameHashValueHashMap() {
		return tagNameHashValueHashMap;
	}
	public void setTagNameHashValueHashMap(
			Map<Integer, Integer> tagNameHashValueHashMap) {
		this.tagNameHashValueHashMap = tagNameHashValueHashMap;
	}
	public Map<String, String> getFieldNameValueMap() {
		return fieldNameValueMap;
	}
	public void setFieldNameValueMap(Map<String, String> fieldNameValueMap) {
		this.fieldNameValueMap = fieldNameValueMap;
	}
}
