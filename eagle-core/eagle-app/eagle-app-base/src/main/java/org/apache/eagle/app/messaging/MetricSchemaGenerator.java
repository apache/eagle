/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.eagle.app.messaging;


import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import com.typesafe.config.Config;
import org.apache.eagle.app.entities.MetricSchemaEntity;
import org.apache.eagle.app.environment.builder.MetricDefinition;
import org.apache.eagle.log.entity.GenericServiceAPIResponseEntity;
import org.apache.eagle.service.client.EagleServiceClientException;
import org.apache.eagle.service.client.impl.EagleServiceClientImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

public class MetricSchemaGenerator extends BaseRichBolt {
    private static final Logger LOG = LoggerFactory.getLogger(MetricSchemaGenerator.class);
    private static int MAX_CACHE_LENGTH = 1000;

    private final MetricDefinition metricDefinition;
    private final Config config;
    private OutputCollector collector;
    private final HashSet<String> metricNameCache = new HashSet<>(MAX_CACHE_LENGTH);
    private EagleServiceClientImpl client;
    private static final HashMap<String, Class<?>> GENERIC_METRIC_VALUE_TYPES = new HashMap<String, Class<?>>() {{
        put(MetricSchemaEntity.GENERIC_METRIC_VALUE_NAME, double.class);
    }};

    public MetricSchemaGenerator(MetricDefinition metricDefinition, Config config) {
        this.metricDefinition = metricDefinition;
        this.config = config;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        this.client = new EagleServiceClientImpl(config);
    }

    @Override
    public void execute(Tuple input) {
        try {
            String metricName = input.getString(0);
            synchronized (metricNameCache) {
                if (!metricNameCache.contains(metricName)) {
                    createMetricSchemaEntity(metricName, this.metricDefinition);
                    metricNameCache.add(metricName);
                }
                if (metricNameCache.size() > MAX_CACHE_LENGTH) {
                    this.metricNameCache.clear();
                }
            }
            this.collector.ack(input);
        } catch (Throwable throwable) {
            LOG.warn(throwable.getMessage(), throwable);
            this.collector.reportError(throwable);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }

    private void createMetricSchemaEntity(String metricName, MetricDefinition metricDefinition) throws IOException, EagleServiceClientException {
        MetricSchemaEntity schemaEntity = new MetricSchemaEntity();
        schemaEntity.setTags(new HashMap<String, String>() {{
            put(MetricSchemaEntity.METRIC_NAME, metricName);
        }});
        schemaEntity.setDimensions(metricDefinition.getDimensionFields());
        schemaEntity.setMetrics(GENERIC_METRIC_VALUE_TYPES);
        GenericServiceAPIResponseEntity<String> response = this.client.create(Collections.singletonList(schemaEntity));
        if (response.isSuccess()) {
            LOG.info("Successfully updated metric schema {}: {}", metricName, schemaEntity);
        } else {
            LOG.error("Server error: {}", response.getException());
        }
    }
}
