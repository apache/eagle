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
package org.apache.eagle.storage.operation;

import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;

/**
 */
public class RawQuery {
    private String query;
    private String startTime;
    private String endTime;
    private int pageSize;
    private String startRowkey;
    private boolean treeAgg;
    private boolean timeSeries;
    private long intervalmin;
    private int top;
    private boolean filterIfMissing;
    private String metricName;
    private boolean verbose;

    public int getParallel() {
        return parallel;
    }

    public void setParallel(int parallel) {
        this.parallel = parallel;
    }

    private int parallel;

    public boolean isVerbose() {
        return verbose;
    }

    public void setVerbose(boolean verbose) {
        this.verbose = verbose;
    }

    public String getQuery() {
        return query;
    }

    public void setQuery(String query) {
        this.query = query;
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

    public int getPageSize() {
        return pageSize;
    }

    public void setPageSize(int pageSize) {
        this.pageSize = pageSize;
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

    public boolean isTimeSeries() {
        return timeSeries;
    }

    public void setTimeSeries(boolean timeSeries) {
        this.timeSeries = timeSeries;
    }

    public long getIntervalmin() {
        return intervalmin;
    }

    public void setIntervalmin(long intervalmin) {
        this.intervalmin = intervalmin;
    }

    public int getTop() {
        return top;
    }

    public void setTop(int top) {
        this.top = top;
    }

    public boolean isFilterIfMissing() {
        return filterIfMissing;
    }

    public void setFilterIfMissing(boolean filterIfMissing) {
        this.filterIfMissing = filterIfMissing;
    }

    public String getMetricName() {
        return metricName;
    }

    public void setMetricName(String metricName) {
        this.metricName = metricName;
    }

    public static Builder build(){
        return new Builder();
    }
    public static class Builder{
        private final RawQuery rawQuery;
        public Builder(){
            this.rawQuery= new RawQuery();
        }
        public RawQuery done(){
            return this.rawQuery;
        }
        public Builder query(String query) {
            this.rawQuery.setQuery(query);
            return this;
        }

        public Builder startTime(String startTime) {
            this.rawQuery.setStartTime(startTime);
            return this;
        }

        public Builder endTime(String endTime) {
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

        public Builder timeSeries(boolean timeSeries) {
            this.rawQuery.setTimeSeries(timeSeries);
            return this;
        }

        public Builder intervalMin(long intervalmin) {
            this.rawQuery.setIntervalmin(intervalmin);
            return this;
        }

        public Builder top(int top) {
            this.rawQuery.setTop(top);
            return this;
        }

        public Builder filerIfMissing(boolean filterIfMissing) {
            this.rawQuery.setFilterIfMissing(filterIfMissing);
            return this;
        }

        public Builder parallel(int parallel) {
            this.rawQuery.setParallel(parallel);
            return this;
        }

        public Builder metricName(String metricName) {
            this.rawQuery.setMetricName(metricName);
            return this;
        }

        public Builder verbose(Boolean verbose) {
            if(verbose == null) verbose = true;
            this.rawQuery.setVerbose(verbose);
            return this;
        }
    }

    @Override
    public String toString() {
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            return objectMapper.writeValueAsString(this);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }
}
