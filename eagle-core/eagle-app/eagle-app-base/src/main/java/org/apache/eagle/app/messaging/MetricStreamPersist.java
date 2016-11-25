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
import org.apache.eagle.app.environment.builder.MetricDefinition;
import org.apache.eagle.app.utils.StreamConvertHelper;
import org.apache.eagle.log.entity.GenericMetricEntity;
import org.apache.eagle.log.entity.GenericServiceAPIResponseEntity;
import org.apache.eagle.service.client.IEagleServiceClient;
import org.apache.eagle.service.client.impl.EagleServiceClientImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class MetricStreamPersist extends BaseRichBolt  {
    private static final Logger LOG = LoggerFactory.getLogger(MetricStreamPersist.class);

    private final Config config;
    private final MetricMapper mapper;
    private IEagleServiceClient client;
    private OutputCollector collector;

    public MetricStreamPersist(MetricDefinition metricDefinition, Config config) {
        this.config = config;
        this.mapper = new DefaultMetricMapper(metricDefinition);
    }

    public MetricStreamPersist(MetricMapper mapper, Config config) {
        this.config = config;
        this.mapper = mapper;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.client = new EagleServiceClientImpl(config);
        this.collector = collector;
    }

    /**
     * TODO: Refactor to persist by batch.
     */
    @Override
    public void execute(Tuple input) {
        try {
            GenericServiceAPIResponseEntity<String> response = this.client.create(
                Collections.singletonList(this.mapper.map(StreamConvertHelper.tupleToMap(input))), GenericMetricEntity.GENERIC_METRIC_SERVICE);
            if (!response.isSuccess()) {
                LOG.error("Service side error: {}", response.getException());
                collector.reportError(new IllegalStateException(response.getException()));
            } else {
                collector.ack(input);
            }
        } catch (Exception ex) {
            LOG.error(ex.getMessage(), ex);
            collector.reportError(ex);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

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


    /**
     * TODO: Not implement yet.
     */
    public class DefaultMetricMapper implements MetricMapper {
        private MetricDefinition metricDefinition;
        private DefaultMetricMapper(MetricDefinition metricDefinition) {
            this.metricDefinition = metricDefinition;
        }

        @Override
        public GenericMetricEntity map(Map event) {
            // TODO: Convert value to Metric entity
            String metricName = null;
            Long timestamp = null;
            Map<String,String> tags = new HashMap<>();
            Map<String,Object> fields = new HashMap<>();
            double[] values = new double[]{};

            GenericMetricEntity entity = new GenericMetricEntity();
            entity.setPrefix(metricName);
            entity.setTimestamp(timestamp);
            entity.setTags(tags);
            entity.setValue(values);
            // return entity;
            throw new IllegalStateException("TODO: Not implemented yet");
        }
    }
}