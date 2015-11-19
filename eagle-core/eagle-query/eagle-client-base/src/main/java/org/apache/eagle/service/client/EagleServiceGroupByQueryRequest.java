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

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.List;

public class EagleServiceGroupByQueryRequest {
	
	private String filter;
	private String startRowkey;
	private int pageSize;
	private String startTime;
	private String endTime;
	private List<String> groupBys;
	private List<String> returns;
	private List<String> orderBys;
	private String metricName;
	private int intervalMin;
	
	public String getMetricName() {
		return metricName;
	}
	public void setMetricName(String metricName) {
		this.metricName = metricName;
	}
	public String getFilter() {
		return filter;
	}
	public void setFilter(String filter) {
		this.filter = filter;
	}
	public String getStartRowkey() {
		return startRowkey;
	}
	public void setStartRowkey(String startRowkey) {
		this.startRowkey = startRowkey;
	}
	public int getPageSize() {
		return pageSize;
	}
	public void setPageSize(int pageSize) {
		this.pageSize = pageSize;
	}
	public String getStartTime() {
		return startTime;
	}
	public void setStartTime(String startTime) {
		this.startTime = startTime;
	}
	public String getEndTime() {
		return endTime;
	}
	public void setEndTime(String endTime) {
		this.endTime = endTime;
	}
	public List<String> getGroupBys() {
		return groupBys;
	}
	public void setGroupBys(List<String> groupBys) {
		this.groupBys = groupBys;
	}
	public List<String> getOrderBys() {
		return orderBys;
	}
	public void setOrderBys(List<String> orderBys) {
		this.orderBys = orderBys;
	}
	public List<String> getReturns() {
		return returns;
	}
	public void setReturns(List<String> returns) {
		this.returns = returns;
	}
	
	public String getQueryParameterString(String service) throws EagleServiceClientException {
		if (pageSize <= 0) {
			throw new EagleServiceClientException("pageSize can't be less than 1, pageSize: " + pageSize);
		}
		try {
			final String query = getQuery();
			final StringBuilder sb = new StringBuilder();
			// query
			sb.append("query=").append(service).append(URLEncoder.encode(query, "UTF-8"));
			// startRowkey
			if (startRowkey != null) {
				sb.append("&startRowkey=").append(startRowkey);
			}
			// pageSize
			sb.append("&pageSize=").append(this.pageSize);
			if (startTime != null || endTime != null) {
				sb.append("&startTime=").append(URLEncoder.encode(startTime, "UTF-8"));
				sb.append("&endTime=").append(URLEncoder.encode(endTime, "UTF-8"));
			}
			
			// metricName
			if(metricName != null){
				sb.append("&metricName=" + metricName);
			}
			
			if (intervalMin != 0) {
				sb.append("&timeSeries=true&intervalmin=" + intervalMin);
			}
			return sb.toString();
		} catch (UnsupportedEncodingException e) {
			throw new EagleServiceClientException("Got an UnsupportedEncodingException" + e.getMessage(), e);
		}
	}
	
	private String getQuery() {
		final StringBuilder sb = new StringBuilder();
		sb.append("[").append(filter).append("]<");
		boolean first = true;
		if (groupBys != null && groupBys.size() > 0) {
			for (String groupBy : groupBys) {
				if (first) {
					first = false;
				} else {
					sb.append(",");
				}
				sb.append("@").append(groupBy);
			}
		}
		sb.append(">{");
		if (returns != null && returns.size() > 0) {
			first = true;
			for (String returnFiled : returns) {
				if (first) {
					first = false;
				} else {
					sb.append(",");
				}
				sb.append(returnFiled);
			}
		}
		sb.append("}");
		if (orderBys != null && orderBys.size() > 0) {
			sb.append(".{");
			first = true;
			for (String orderBy : orderBys) {
				if (first) {
					first = false;
				} else {
					sb.append(",");
				}
				sb.append(orderBy);
			}
			sb.append("}");
		}
		return sb.toString();
	}
	public int getIntervalMin() {
		return intervalMin;
	}
	public void setIntervalMin(int intervalMin) {
		this.intervalMin = intervalMin;
	}
}
