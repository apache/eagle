/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/

package org.apache.eagle.jpm.aggregation.mr;

import org.apache.eagle.jpm.aggregation.AggregationConfig;
import org.apache.eagle.jpm.aggregation.common.AggregatorColumns;
import org.apache.eagle.jpm.aggregation.common.MetricAggregator;
import org.apache.eagle.jpm.util.Constants;
import org.apache.eagle.log.entity.GenericMetricEntity;
import org.apache.eagle.log.entity.GenericServiceAPIResponseEntity;
import org.apache.eagle.service.client.IEagleServiceClient;
import org.apache.eagle.service.client.impl.EagleServiceClientImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.*;

import static org.apache.eagle.jpm.aggregation.AggregationConfig.EagleServiceConfig;

public class MRMetricAggregator implements MetricAggregator, Serializable {
    private static final Logger LOG = LoggerFactory.getLogger(MRMetricAggregator.class);

    private String metric;
    private List<List<String>> aggregateColumns;
    //key is AggregatorColumns, value is a map(key is timeStamp, value is metric value)
    private Map<AggregatorColumns, Map<Long, Long>> aggregateValues;
    private AggregationConfig appConfig;
    private EagleServiceConfig eagleServiceConfig;

    public MRMetricAggregator(String metric, List<List<String>> aggregateColumns, AggregationConfig appConfig) {
        this.metric = metric;
        this.aggregateColumns = aggregateColumns;
        this.aggregateValues = new TreeMap<>();
        this.appConfig = appConfig;
        eagleServiceConfig = appConfig.getEagleServiceConfig();
    }

    @Override
    public boolean aggregate(long startTime, long endTime) {
        LOG.info("start to aggregate {} from {} to {}", metric, startTime, endTime);
        IEagleServiceClient client = new EagleServiceClientImpl(
                eagleServiceConfig.eagleServiceHost,
                eagleServiceConfig.eagleServicePort,
                eagleServiceConfig.username,
                eagleServiceConfig.password);

        String query = String.format("%s[@site=\"%s\"]{*}",
            Constants.GENERIC_METRIC_SERVICE,
            appConfig.getStormConfig().site);

        GenericServiceAPIResponseEntity response;
        try {
            response = client
                .search(query)
                .metricName(String.format(Constants.HADOOP_HISTORY_MINUTE_METRIC_FORMAT, Constants.JOB_LEVEL, metric))
                .startTime(startTime)
                .endTime(endTime)
                .pageSize(Integer.MAX_VALUE)
                .send();

            client.close();
        } catch (Exception e) {
            LOG.warn("{}", e);
            return false;
        }

        List<GenericMetricEntity> entities = response.getObj();
        for (GenericMetricEntity entity : entities) {
            Map<String, String> tags = entity.getTags();
            for (List<String> columnNames : this.aggregateColumns) {
                List<String> columnValues = new ArrayList<>();
                boolean allContains = true;
                for (String columnName : columnNames) {
                    if (tags.containsKey(columnName)) {
                        columnValues.add(tags.get(columnName));
                    } else {
                        LOG.warn("metric entity tags do not contain {}", columnName);
                        allContains = false;
                        break;
                    }
                }

                if (!allContains) {
                    continue;
                }

                AggregatorColumns aggregatorColumns = new AggregatorColumns(columnNames, columnValues);
                if (!this.aggregateValues.containsKey(aggregatorColumns)) {
                    this.aggregateValues.put(aggregatorColumns, new HashMap<>());
                }

                if (!this.aggregateValues.get(aggregatorColumns).containsKey(entity.getTimestamp())) {
                    this.aggregateValues.get(aggregatorColumns).put(entity.getTimestamp(), 0L);
                }

                Long previous = this.aggregateValues.get(aggregatorColumns).get(entity.getTimestamp());
                this.aggregateValues.get(aggregatorColumns).put(entity.getTimestamp(), previous + (long)entity.getValue()[0]);
            }
        }
        return flush();
    }

    private String buildMetricName(List<String> columnNames) {
        return String.format(Constants.HADOOP_HISTORY_MINUTE_METRIC_FORMAT, columnNames.get(columnNames.size() - 1).toLowerCase(), metric);
    }

    private boolean flush() {
        IEagleServiceClient client = new EagleServiceClientImpl(
                eagleServiceConfig.eagleServiceHost,
                eagleServiceConfig.eagleServicePort,
                eagleServiceConfig.username,
                eagleServiceConfig.password);

        List<GenericMetricEntity> entities = new ArrayList<>();
        for (AggregatorColumns aggregatorColumns : this.aggregateValues.keySet()) {
            List<String> columnNames = aggregatorColumns.getColumnNames();
            List<String> columnValues = aggregatorColumns.getColumnValues();
            Map<String, String> baseTags = new HashMap<>();
            for (int i = 0; i < columnNames.size(); ++i) {
                baseTags.put(columnNames.get(i), columnValues.get(i));
            }

            Map<Long, Long> metricByMin = this.aggregateValues.get(aggregatorColumns);
            for (Long timeStamp : metricByMin.keySet()) {
                GenericMetricEntity metricEntity = new GenericMetricEntity();
                metricEntity.setTimestamp(timeStamp);
                metricEntity.setPrefix(buildMetricName(columnNames));
                metricEntity.setValue(new double[] {metricByMin.get(timeStamp)});
                metricEntity.setTags(baseTags);
                entities.add(metricEntity);
            }
        }

        try {
            LOG.info("start flushing entities of total number " + entities.size());
            client.create(entities);
            LOG.info("finish flushing entities of total number " + entities.size());
            entities.clear();
            client.close();
        } catch (Exception e) {
            LOG.warn("{}", e);
            return false;
        }
        return true;
    }
}
