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

import org.apache.eagle.log.entity.GenericMetricEntity;
import org.apache.eagle.service.client.IEagleServiceClient;
import org.apache.eagle.service.client.EagleServiceClientException;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class MetricSender extends BatchSender {
    private final String metricName;
    private Map<String, String> tags;

    public MetricSender(IEagleServiceClient client, String metricName) {
        super(client,1);
        this.metricName = metricName;
    }

    public MetricSender batch(int batchSize) {
        super.setBatchSize(batchSize);
        return this;
    }

    public MetricSender tags(Map<String,String> tags){
        this.tags = tags;
        return this;
    }

    public MetricSender send(String metricName,long timestamp,Map<String,String> tags,double ...values) throws IOException, EagleServiceClientException {
        GenericMetricEntity metric = new GenericMetricEntity();
        metric.setPrefix(metricName);
        metric.setValue(values);
        metric.setTimestamp(timestamp);
        metric.setTags(tags);
        super.send(metric);
        return this;
    }

    public MetricSender send(Long timestamp,Map<String,String> tags,double ...values) throws IOException, EagleServiceClientException {
        return this.send(this.metricName,timestamp,tags,values);
    }

    public MetricSender send(Long timestamp, double ... values) throws IOException, EagleServiceClientException {
        return this.send(timestamp,new HashMap<String, String>(this.tags),values);
    }

//    public EagleServiceMetricSender send(String metricName,Map<String,String> tags,double ... values) throws IOException, EagleServiceClientException {
//        return this.send(metricName,System.currentTimeMillis(),tags,values);
//    }
//
//    public EagleServiceMetricSender send(Map<String,String> tags,double ...values) throws IOException, EagleServiceClientException {
//        return this.send(this.metricName,System.currentTimeMillis(),tags,values);
//    }
//
//    public EagleServiceMetricSender send(double ... values) throws IOException, EagleServiceClientException {
//        return this.send(System.currentTimeMillis(), new HashMap<String, String>(this.tags), values);
//    }
}