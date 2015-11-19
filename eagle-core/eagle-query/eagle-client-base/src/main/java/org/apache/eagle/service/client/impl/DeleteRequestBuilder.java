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
import org.apache.eagle.service.client.EagleServiceClientException;
import org.apache.eagle.service.client.EagleServiceSingleEntityQueryRequest;
import org.apache.eagle.service.client.IEagleServiceClient;
import org.apache.eagle.common.DateTimeUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.List;

public class DeleteRequestBuilder {
    private final IEagleServiceClient client;
    private List<String> deleteIds = null;
    private EagleServiceSingleEntityQueryRequest request;

    private final static Logger LOG = LoggerFactory.getLogger(DeleteRequestBuilder.class);
    private String serviceName;

    public DeleteRequestBuilder(IEagleServiceClient client){
        this.client = client;
    }

    public DeleteRequestBuilder byId(List<String> ids){
        this.deleteIds = ids;
        return this;
    }

    public DeleteRequestBuilder byQuery(String query){
        if(this.request==null)  this.request = new EagleServiceSingleEntityQueryRequest();
        try {
            this.request.setQuery(URLEncoder.encode(query, "UTF-8"));
        } catch (UnsupportedEncodingException e) {
            LOG.error(e.getMessage(),e);
        }
        return this;
    }

    public DeleteRequestBuilder serviceName(String serviceName){
        this.serviceName = serviceName;
        return this;
    }

    public DeleteRequestBuilder startRowkey(String startRowkey){
        if(this.request==null)  this.request = new EagleServiceSingleEntityQueryRequest();
        this.request.setStartRowkey(startRowkey);
        return this;
    }

    public DeleteRequestBuilder pageSize(int pageSize){
        if(this.request==null)  this.request = new EagleServiceSingleEntityQueryRequest();
        this.request.setPageSize(pageSize);
        return this;
    }

    public DeleteRequestBuilder startTime(long startTime){
        if(this.request==null)  this.request = new EagleServiceSingleEntityQueryRequest();
        this.request.setStartTime(startTime);
        return this;
    }

    public DeleteRequestBuilder startTime(String startTime){
        if(this.request==null)  this.request = new EagleServiceSingleEntityQueryRequest();
        this.request.setStartTime(DateTimeUtil.humanDateToMillisecondsWithoutException(startTime));
        return this;
    }

    public DeleteRequestBuilder endTime(long endTime){
        if(this.request==null)  this.request = new EagleServiceSingleEntityQueryRequest();
        this.request.setEndTime(endTime);
        return this;
    }

    public DeleteRequestBuilder endTime(String endTime){
        if(this.request==null)  this.request = new EagleServiceSingleEntityQueryRequest();
        this.request.setEndTime(DateTimeUtil.humanDateToMillisecondsWithoutException(endTime));
        return this;
    }

    public DeleteRequestBuilder timeRange(String startTime,String endTime){
        this.startTime(startTime);
        this.endTime(endTime);
        return this;
    }

    public DeleteRequestBuilder timeRange(long startTime,long endTime){
        this.startTime(startTime);
        this.endTime(endTime);
        return this;
    }

    public DeleteRequestBuilder treeAgg(boolean treeAgg){
        if(this.request==null)  this.request = new EagleServiceSingleEntityQueryRequest();
        this.request.setTreeAgg(treeAgg);
        return this;
    }

    public DeleteRequestBuilder metricName(String metricName){
        if(this.request==null)  this.request = new EagleServiceSingleEntityQueryRequest();
        this.request.setMetricName(metricName);
        return this;
    }

    public DeleteRequestBuilder filterIfMissing(Boolean filterIfMissing){
        if(this.request==null)  this.request = new EagleServiceSingleEntityQueryRequest();
        this.request.setFilterIfMissing(filterIfMissing);
        return this;
    }

    public GenericServiceAPIResponseEntity<String> send() throws EagleServiceClientException, IOException {
        if(this.deleteIds!=null){
            return client.deleteById(this.deleteIds,this.serviceName);
        }else {
            return client.delete(this.request);
        }
    }
}