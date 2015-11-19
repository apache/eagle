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
package org.apache.eagle.service.client;

import java.util.List;
import java.util.Map;

import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.map.annotate.JsonSerialize;

//@JsonPropertyOrder({ "success", "exception", "elapsems", "totalResults", "elapsedms", "obj", "lastTimestamp" })
@JsonSerialize(include=JsonSerialize.Inclusion.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class TimeSeriesGroupByAPIResponseEntity {
	private boolean success;
	private String exception;
	private int totalResults;
	private long elapsedms;
	private List<Entry> obj;
	private long lastTimestamp;

	public long getElapsedms() {
		return elapsedms;
	}
	public void setElapsedms(long elapsedms) {
		this.elapsedms = elapsedms;
	}
	
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
	public int getTotalResults() {
		return totalResults;
	}
	public void setTotalResults(int totalResults) {
		this.totalResults = totalResults;
	}
	public List<Entry> getObj() {
		return obj;
	}
	public void setObj(List<Entry> obj) {
		this.obj = obj;
	}
	
	public long getLastTimestamp() {
		return lastTimestamp;
	}
	public void setLastTimestamp(long lastTimestamp) {
		this.lastTimestamp = lastTimestamp;
	}

	public static class Entry implements Map.Entry<List<String>, List<Double[]>> {

		private List<String> key;
		private List<Double[]> value;
		public List<String> getKey() {
			return key;
		}
		public void setKey(List<String> key) {
			this.key = key;
		}

		@Override
		public List<Double[]> getValue() {
			return value;
		}

		@Override
		public List<Double[]> setValue(List<Double[]> value) {
			List<Double[]> old = this.value;
			this.value = value;
			return old;
		}
	}
}
