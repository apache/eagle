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

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import com.google.common.base.Preconditions;
import com.typesafe.config.Config;
import org.apache.eagle.app.environment.builder.MetricDescriptor;
import org.apache.eagle.app.utils.StreamConvertHelper;
import org.apache.eagle.common.DateTimeUtil;
import org.apache.eagle.log.entity.GenericMetricEntity;
import org.apache.eagle.log.entity.GenericServiceAPIResponseEntity;
import org.apache.eagle.service.client.IEagleServiceClient;
import org.apache.eagle.service.client.impl.BatchSender;
import org.apache.eagle.service.client.impl.EagleServiceClientImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class MetricStreamPersist extends BaseRichBolt {
    private static final Logger LOG = LoggerFactory.getLogger(MetricStreamPersist.class);
    public static final String METRIC_NAME_FIELD = "metricName";
    public static final String METRIC_EVENT_FIELD = "metricEvent";

    private final Config config;
    private final MetricMapper mapper;
    private final int batchSize;
    private IEagleServiceClient client;
    private OutputCollector collector;
    private BatchSender batchSender;

    public MetricStreamPersist(MetricDescriptor metricDescriptor, Config config) {
        this.config = config;
        this.mapper = new StructuredMetricMapper(metricDescriptor);
        this.batchSize = config.hasPath("service.batchSize") ? config.getInt("service.batchSize") : 1;
    }

    public MetricStreamPersist(MetricMapper mapper, Config config) {
        this.config = config;
        this.mapper = mapper;
        this.batchSize = config.hasPath("service.batchSize") ? config.getInt("service.batchSize") : 1;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.client = new EagleServiceClientImpl(config);
        if (this.batchSize > 0) {
            this.batchSender = client.batch(this.batchSize);
        }
        this.collector = collector;
    }

    @Override
    public void execute(Tuple input) {
        GenericMetricEntity metricEntity = null;
        Map event = null;
        try {
            event = StreamConvertHelper.tupleToEvent(input).f1();
            metricEntity = this.mapper.map(event);
            if (batchSize <= 1) {
                GenericServiceAPIResponseEntity<String> response = this.client.create(Collections.singletonList(metricEntity));
                if (!response.isSuccess()) {
                    LOG.error("Service side error: {}", response.getException());
                    collector.reportError(new IllegalStateException(response.getException()));
                }
            } else {
                this.batchSender.send(metricEntity);
            }
        } catch (Exception ex) {
            LOG.error(ex.getMessage(), ex);
            collector.reportError(ex);
        } finally {
            if (metricEntity != null && event != null) {
                collector.emit(Arrays.asList(metricEntity.getPrefix(), event));
            }
            collector.ack(input);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(METRIC_NAME_FIELD, METRIC_EVENT_FIELD));
    }

    @Override
    public void cleanup() {
        try {
            this.client.close();
        } catch (IOException e) {
            LOG.error("Close client error: {}", e.getMessage(), e);
        } finally {
            super.cleanup();
        }
    }

    @FunctionalInterface
    public interface MetricMapper extends Serializable {
        GenericMetricEntity map(Map event);
    }

    public class StructuredMetricMapper implements MetricMapper {
        private final MetricDescriptor metricDescriptor;

        private StructuredMetricMapper(MetricDescriptor metricDescriptor) {
            this.metricDescriptor = metricDescriptor;
        }

        @Override
        public GenericMetricEntity map(Map event) {
            String metricName = metricDescriptor.getMetricNameSelector().getMetricName(event);
            Preconditions.checkNotNull(metricName, "Metric name is null");
            Long timestamp = metricDescriptor.getTimestampSelector().getTimestamp(event);
            Preconditions.checkNotNull(timestamp, "Timestamp is null");
            Map<String, String> tags = new HashMap<>();
            for (String dimensionField : metricDescriptor.getDimensionFields()) {
                Preconditions.checkNotNull(dimensionField, "Dimension field name is null");
                tags.put(dimensionField, (String) event.get(dimensionField));
            }

            double[] values;
            if (event.containsKey(metricDescriptor.getValueField())) {
                values = new double[] {(double) event.get(metricDescriptor.getValueField())};
            } else {
                LOG.warn("Event has no value field '{}': {}, use 0 by default", metricDescriptor.getValueField(), event);
                values = new double[] {0};
            }

            GenericMetricEntity entity = new GenericMetricEntity();
            entity.setPrefix(metricName);
            entity.setTimestamp(DateTimeUtil.roundDown(metricDescriptor.getGranularity(), timestamp));
            entity.setTags(tags);
            entity.setValue(values);
            return entity;
        }
    }
}