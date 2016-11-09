/*
 *
 *    Licensed to the Apache Software Foundation (ASF) under one or more
 *    contributor license agreements.  See the NOTICE file distributed with
 *    this work for additional information regarding copyright ownership.
 *    The ASF licenses this file to You under the Apache License, Version 2.0
 *    (the "License"); you may not use this file except in compliance with
 *    the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 *
 */

package org.apache.eagle.metric.reportor;

import org.apache.eagle.log.entity.GenericMetricEntity;
import org.apache.eagle.log.entity.GenericServiceAPIResponseEntity;
import org.apache.eagle.service.client.impl.EagleServiceClientImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class EagleServiceReporterMetricListener implements EagleMetricListener {

    private EagleServiceClientImpl client;
    private static final Logger LOG = LoggerFactory.getLogger(EagleServiceReporterMetricListener.class);

    public EagleServiceReporterMetricListener(String host, int port, String username, String password) {
        client = new EagleServiceClientImpl(host, port, username, password);
    }

    public EagleServiceReporterMetricListener(String host, int port) {
        client = new EagleServiceClientImpl(host, port, null, null);
    }

    public void onMetricFlushed(List<EagleMetric> metrics) {
        List<GenericMetricEntity> entities = new ArrayList<>();
        for (EagleMetric metric : metrics) {
            String metricName = metric.name;
            entities.add(MetricEntityAdaptor.convert(metricName, metric));
        }
        try {
            int total = entities.size();
            GenericServiceAPIResponseEntity<String> response = client.create(entities, GenericMetricEntity.GENERIC_METRIC_SERVICE);
            if (response.isSuccess()) {
                LOG.info("Wrote " + total + " entities to service");
            } else {
                LOG.error("Failed to write " + total + " entities to service, due to server exception: " + response.getException());
            }
        } catch (Exception ex) {
            LOG.error("Got exception while writing entities: ", ex);
        }
    }
}
