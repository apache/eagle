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

public class EagleServiceSingleEntityQueryRequest {
	private String query;
	private String startRowkey;
	private int pageSize;
	private long startTime;
	private long endTime;
	private boolean treeAgg;
	private String metricName;
	private boolean filterIfMissing;

    public boolean isVerbose() {
        return verbose;
    }

    public void setVerbose(boolean verbose) {
        this.verbose = verbose;
    }

    private boolean verbose;
	
	public String getMetricName() {
		return metricName;
	}

	public void setMetricName(String metricName) {
		this.metricName = metricName;
	}

	public String getQuery() {
		return query;
	}

	public void setQuery(String query) {
		this.query = query;
	}

	public String getStartRowkey() {
		return startRowkey;
	}

	public void setStartRowkey(String startRowkey) {
		this.startRowkey = startRowkey;
	}

	public boolean isTreeAgg() {
		return treeAgg;
	}

	public void setTreeAgg(boolean treeAgg) {
		this.treeAgg = treeAgg;
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
	
	public boolean getFilterIfMissing() {
		return filterIfMissing;
	}

	public void setFilterIfMissing(boolean filterIfMissing) {
		this.filterIfMissing = filterIfMissing;
	}

	public String getQueryParameterString() throws EagleServiceClientException {
		if (pageSize < 0) {
			throw new EagleServiceClientException("pageSize can't be less than 0, pageSize: " + pageSize);
		}

		// startTime and endTime is optional
		
		final StringBuilder sb = new StringBuilder();
		// query
//        try {
//            sb.append("query=").append(URLEncoder.encode(query,"UTF-8"));
            sb.append("query=").append(query);
//        } catch (UnsupportedEncodingException e) {
//            throw new EagleServiceClientException(e);
//        }

        // startRowkey
		if (startRowkey != null) {
			sb.append("&startRowkey=").append(startRowkey);
		}
		// pageSize
		sb.append("&pageSize=").append(this.pageSize);
		if (startTime !=0 || endTime != 0) {
			Date date = new Date(startTime);
			SimpleDateFormat dateFormatter = new SimpleDateFormat("yyyy-MM-dd%20HH:mm:ss");
            dateFormatter.setTimeZone(EagleConfigFactory.load().getTimeZone());
			String timeString = dateFormatter.format(date);
			sb.append("&startTime=").append(timeString);
			date.setTime(endTime);
			timeString = dateFormatter.format(date);
			sb.append("&endTime=").append(timeString);
		}
		// tree aggregate
		sb.append("&treeAgg=").append(treeAgg);
		
		// for metric read
		if(metricName != null){
			sb.append("&metricName=").append(metricName);
		}
		
		if (filterIfMissing == true) { 
			sb.append("&filterIfMissing=").append("true");
		}
		return sb.toString();
	}

    public static Builder build(){
        return new Builder();
    }

    public static class Builder{
        private final EagleServiceSingleEntityQueryRequest rawQuery;
        public Builder(){
            this.rawQuery= new EagleServiceSingleEntityQueryRequest();
        }
        public EagleServiceSingleEntityQueryRequest done(){
            return this.rawQuery;
        }
        public Builder query(String query) {
            this.rawQuery.setQuery(query);
            return this;
        }

        public Builder startTime(long startTime) {
            this.rawQuery.setStartTime(startTime);
            return this;
        }

        public Builder endTime(long endTime) {
            this.rawQuery.setEndTime(endTime);
            return this;
        }

        public Builder pageSize(int pageSize) {
            this.rawQuery.setPageSize(pageSize);
            return this;
        }

        public Builder startRowkey(String startRowkey) {
            this.rawQuery.setStartRowkey(startRowkey);
            return this;
        }

        public Builder treeAgg(boolean treeAgg) {
            this.rawQuery.setTreeAgg(treeAgg);
            return this;
        }

        public Builder filerIfMissing(boolean filterIfMissing) {
            this.rawQuery.setFilterIfMissing(filterIfMissing);
            return this;
        }

        public Builder metricName(String metricName) {
            this.rawQuery.setMetricName(metricName);
            return this;
        }

        public Builder verbose(Boolean verbose) {
            this.rawQuery.setVerbose(verbose);
            return this;
        }
    }
}
