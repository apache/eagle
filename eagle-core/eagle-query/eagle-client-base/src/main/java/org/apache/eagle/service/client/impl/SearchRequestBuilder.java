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
package org.apache.eagle.service.client.impl;

import org.apache.eagle.log.entity.GenericServiceAPIResponseEntity;
import org.apache.eagle.service.client.IEagleServiceClient;
import org.apache.eagle.service.client.EagleServiceClientException;
import org.apache.eagle.service.client.EagleServiceSingleEntityQueryRequest;
import org.apache.eagle.common.DateTimeUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;

public class SearchRequestBuilder {
    private final EagleServiceSingleEntityQueryRequest request;
    private final IEagleServiceClient client;
    private final static Logger LOG = LoggerFactory.getLogger(SearchRequestBuilder.class);

    public SearchRequestBuilder(IEagleServiceClient client) {
        this.request = new EagleServiceSingleEntityQueryRequest();
        this.client = client;
    }

    public SearchRequestBuilder query(String query){
        try {
            this.request.setQuery(URLEncoder.encode(query,"UTF-8"));
        } catch (UnsupportedEncodingException e) {
            LOG.error(e.getMessage(),e);
        }
        return this;
    }

    public SearchRequestBuilder startRowkey(String startRowkey){
        this.request.setStartRowkey(startRowkey);
        return this;
    }

    public SearchRequestBuilder pageSize(int pageSize){
        this.request.setPageSize(pageSize);
        return this;
    }

    public SearchRequestBuilder startTime(long startTime){
        this.request.setStartTime(startTime);
        return this;
    }

    public SearchRequestBuilder startTime(String startTime){
        this.request.setStartTime(DateTimeUtil.humanDateToMillisecondsWithoutException(startTime));
        return this;
    }

    public SearchRequestBuilder endTime(long endTime){
        this.request.setEndTime(endTime);
        return this;
    }

    public SearchRequestBuilder endTime(String endTime){
        this.request.setEndTime(DateTimeUtil.humanDateToMillisecondsWithoutException(endTime));
        return this;
    }

    public SearchRequestBuilder treeAgg(boolean treeAgg){
        this.request.setTreeAgg(treeAgg);
        return this;
    }

    public SearchRequestBuilder metricName(String metricName){
        this.request.setMetricName(metricName);
        return this;
    }

    public SearchRequestBuilder filterIfMissing(Boolean filterIfMissing){
        this.request.setFilterIfMissing(filterIfMissing);
        return this;
    }

    public <T> GenericServiceAPIResponseEntity<T> send() throws EagleServiceClientException {
        return client.search(this.request);
    }
}
