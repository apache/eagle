/*
 *
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
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
package org.apache.eagle.gc.model;

import java.util.HashMap;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * timestamp 		    	long
 * eventType        		string
 * pausedGCTimeSec 	    	double
 * youngAreaGCed	     	boolean
 * youngUsedHeapK   		long
 * youngTotalHeapK   		long
 * tenuredAreaGCed  		boolean
 * tenuredUsedHeapK  		long
 * tenuredTotalHeapK 		long
 * permAreaGCed	      		boolean
 * permUsedHeapK	    	long
 * permTotalHeapK   		long
 * totalHeapUsageAvailable  boolean
 * usedTotalHeapK	   		long
 * totalHeapK		   		long
 * logLine					string
 */

public class GCPausedEvent {

	private long timestamp;
	private String eventType;
	private double pausedGCTimeSec;

	private boolean youngAreaGCed;
	private long youngUsedHeapK;
	private long youngTotalHeapK;

	private boolean tenuredAreaGCed;
	private long tenuredUsedHeapK;
	private long tenuredTotalHeapK;

	private boolean permAreaGCed;
	private long permUsedHeapK;
	private long permTotalHeapK;

	private boolean totalHeapUsageAvailable;
	private long usedTotalHeapK;
	private long totalHeapK;
	private String logLine;

	public long getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}

	public String getEventType() {
		return eventType;
	}

	public void setEventType(String eventType) {
		this.eventType = eventType;
	}

	public double getPausedGCTimeSec() {
		return pausedGCTimeSec;
	}

	public void setPausedGCTimeSec(double pausedGCTimeSec) {
		this.pausedGCTimeSec = pausedGCTimeSec;
	}

	public boolean isYoungAreaGCed() {
		return youngAreaGCed;
	}

	public void setYoungAreaGCed(boolean youngAreaGCed) {
		this.youngAreaGCed = youngAreaGCed;
	}

	public long getYoungUsedHeapK() {
		return youngUsedHeapK;
	}

	public void setYoungUsedHeapK(long youngUsedHeapK) {
		this.youngUsedHeapK = youngUsedHeapK;
	}

	public long getYoungTotalHeapK() {
		return youngTotalHeapK;
	}

	public void setYoungTotalHeapK(long youngTotalHeapK) {
		this.youngTotalHeapK = youngTotalHeapK;
	}

	public boolean isTenuredAreaGCed() {
		return tenuredAreaGCed;
	}

	public void setTenuredAreaGCed(boolean tenuredAreaGCed) {
		this.tenuredAreaGCed = tenuredAreaGCed;
	}

	public long getTenuredUsedHeapK() {
		return tenuredUsedHeapK;
	}

	public void setTenuredUsedHeapK(long tenuredUsedHeapK) {
		this.tenuredUsedHeapK = tenuredUsedHeapK;
	}

	public long getTenuredTotalHeapK() {
		return tenuredTotalHeapK;
	}

	public void setTenuredTotalHeapK(long tenuredTotalHeapK) {
		this.tenuredTotalHeapK = tenuredTotalHeapK;
	}

	public boolean isPermAreaGCed() {
		return permAreaGCed;
	}

	public void setPermAreaGCed(boolean permAreaGCed) {
		this.permAreaGCed = permAreaGCed;
	}

	public long getPermUsedHeapK() {
		return permUsedHeapK;
	}

	public void setPermUsedHeapK(long permUsedHeapK) {
		this.permUsedHeapK = permUsedHeapK;
	}

	public long getPermTotalHeapK() {
		return permTotalHeapK;
	}

	public void setPermTotalHeapK(long permTotalHeapK) {
		this.permTotalHeapK = permTotalHeapK;
	}

	public String getLogLine() {
		return logLine;
	}

	public void setLogLine(String logLine) {
		this.logLine = logLine;
	}

	public boolean isTotalHeapUsageAvailable() {
		return totalHeapUsageAvailable;
	}

	public void setTotalHeapUsageAvailable(boolean totalHeapUsageAvailable) {
		this.totalHeapUsageAvailable = totalHeapUsageAvailable;
	}

	public long getUsedTotalHeapK() {
		return usedTotalHeapK;
	}

	public void setUsedTotalHeapK(long usedTotalHeapK) {
		this.usedTotalHeapK = usedTotalHeapK;
	}

	public long getTotalHeapK() {
		return totalHeapK;
	}

	public void setTotalHeapK(long totalHeapK) {
		this.totalHeapK = totalHeapK;
	}

	public GCPausedEvent() {

	}

	public GCPausedEvent(Map<String, Object> map) {
		this.timestamp = (Long)map.get("timestamp");
		this.eventType = (String)map.get("eventType");
		this.pausedGCTimeSec = (Double)map.get("pausedGCTimeSec");

		this.youngAreaGCed = (Boolean)map.get("youngAreaGCed");
		this.youngUsedHeapK = (Long)map.get("youngUsedHeapK");
		this.youngTotalHeapK = (Long)map.get("youngTotalHeapK");

		this.tenuredAreaGCed = (Boolean)map.get("tenuredAreaGCed");
		this.tenuredUsedHeapK = (Long)map.get("tenuredUsedHeapK");
		this.tenuredTotalHeapK = (Long)map.get("tenuredTotalHeapK");

		this.permAreaGCed = (Boolean)map.get("permAreaGCed");
		this.permUsedHeapK = (Long)map.get("permUsedHeapK");
		this.permTotalHeapK = (Long)map.get("permTotalHeapK");

		this.totalHeapUsageAvailable = (Boolean)map.get("totalHeapUsageAvailable");
		this.usedTotalHeapK = (Long)map.get("usedTotalHeapK");
		this.totalHeapK = (Long)map.get("totalHeapK");

		this.logLine = (String)map.get("logLine");
	}

	public SortedMap<String, Object> toMap() {
		SortedMap<String, Object> map = new TreeMap<>();
		map.put("timestamp", timestamp);
		map.put("eventType", eventType);
		map.put("pausedGCTimeSec", pausedGCTimeSec);

		map.put("youngAreaGCed", youngAreaGCed);
		map.put("youngUsedHeapK", youngUsedHeapK);
		map.put("youngTotalHeapK", youngTotalHeapK);

		map.put("tenuredAreaGCed", tenuredAreaGCed);
		map.put("tenuredUsedHeapK", tenuredUsedHeapK);
		map.put("tenuredTotalHeapK", tenuredTotalHeapK);

		map.put("permAreaGCed",  permAreaGCed);
		map.put("permUsedHeapK", permUsedHeapK);
		map.put("permTotalHeapK", permTotalHeapK);

		map.put("totalHeapUsageAvailable", totalHeapUsageAvailable);
		map.put("usedTotalHeapK", usedTotalHeapK);
		map.put("totalHeapK", totalHeapK);

		map.put("logLine", logLine);
		return map;
	}
}
