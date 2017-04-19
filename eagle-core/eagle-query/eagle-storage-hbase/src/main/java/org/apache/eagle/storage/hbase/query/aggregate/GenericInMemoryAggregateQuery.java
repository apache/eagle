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
package org.apache.eagle.storage.hbase.query.aggregate;

import org.apache.eagle.log.entity.*;
import org.apache.eagle.log.entity.meta.EntityDefinition;
import org.apache.eagle.log.entity.meta.EntityDefinitionManager;
import org.apache.eagle.query.GenericQuery;
import org.apache.eagle.query.aggregate.AggregateCondition;
import org.apache.eagle.query.aggregate.AggregateFunctionType;
import org.apache.eagle.query.aggregate.timeseries.*;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class GenericInMemoryAggregateQuery implements GenericQuery {
    private static final Logger LOG = LoggerFactory.getLogger(GenericCoprocessorAggregateQuery.class);
    private final List<AggregateFunctionType> sortFuncs;
    private final List<String> sortFields;
    private final String serviceName;

    private EntityDefinition entityDef;
    private SearchCondition searchCondition;
    private AggregateCondition aggregateCondition;
    private String prefix;
    private long lastTimestamp = 0;
    private long firstTimestamp = 0;
    private List<SortOption> sortOptions;
    private int top;

    private int aggFuncNum;
    private int sortAggFuncNum;
    private int sortFuncNum;

    public GenericInMemoryAggregateQuery(String serviceName, SearchCondition condition,
                                         AggregateCondition aggregateCondition, String metricName,
                                         List<SortOption> sortOptions, List<AggregateFunctionType> sortFunctionTypes, List<String> sortFields, int top)
            throws InstantiationException, IllegalAccessException {
        checkNotNull(serviceName, "serviceName");
        this.searchCondition = condition;
        this.entityDef = EntityDefinitionManager.getEntityByServiceName(serviceName);
        checkNotNull(entityDef, "EntityDefinition");
        checkNotNull(entityDef, "GroupAggregateCondition");
        this.aggregateCondition = aggregateCondition;
        this.aggFuncNum = this.aggregateCondition.getAggregateFunctionTypes().size();
        this.sortOptions = sortOptions;
        this.sortFuncs = sortFunctionTypes;
        this.sortFuncNum = this.sortOptions == null ? 0 : this.sortOptions.size();
        this.sortFields = sortFields;
        this.top = top;
        this.serviceName = serviceName;

        if (serviceName.equals(GenericMetricEntity.GENERIC_METRIC_SERVICE)) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("list metric aggregate query");
            }
            if (metricName == null || metricName.isEmpty()) {
                throw new IllegalArgumentException("metricName should not be empty for metric list query");
            }
            if (!condition.getOutputFields().contains(GenericMetricEntity.VALUE_FIELD)) {
                condition.getOutputFields().add(GenericMetricEntity.VALUE_FIELD);
            }
            this.prefix = metricName;
        } else {
            if (LOG.isDebugEnabled()) {
                LOG.debug("list entity aggregate query");
            }
            this.prefix = entityDef.getPrefix();
        }

        // Add sort oriented aggregation functions into aggregateCondtion
        if (this.sortOptions != null) {
            // if sort for time series aggregation
            if (this.aggregateCondition.isTimeSeries()) {
                this.sortAggFuncNum = 0;
                int index = 0;
                for (SortOption sortOption : this.sortOptions) {
                    if (!sortOption.isInGroupby()) {
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("Add additional aggregation functions for sort options " + sortOption.toString() + " in index: " + (this.aggFuncNum + this.sortAggFuncNum));
                        }
                        AggregateFunctionType _sortFunc = this.sortFuncs.get(index);
                        if (AggregateFunctionType.avg.equals(_sortFunc)) {
                            this.aggregateCondition.getAggregateFunctionTypes().add(AggregateFunctionType.sum);
                        } else {
                            this.aggregateCondition.getAggregateFunctionTypes().add(_sortFunc);
                        }
                        this.aggregateCondition.getAggregateFields().add(this.sortFields.get(index));

                        sortOption.setIndex(this.sortAggFuncNum);
                        sortAggFuncNum++;
                    }
                    index++;
                }
            }
        }
    }

    private void checkNotNull(Object o, String message) {
        if (o == null) {
            throw new IllegalArgumentException(message + " should not be null");
        }
    }

    @Override
    public <T> List<T> result() throws Exception {
        // non time-series based aggregate query, not hierarchical
        final List<String> groupbyFields = aggregateCondition.getGroupbyFields();
        final List<String> aggregateFields = aggregateCondition.getAggregateFields();
        final List<String> filterFields = searchCondition.getOutputFields();
        final List<String> outputFields = new ArrayList<>();
        if (groupbyFields != null) {
            outputFields.addAll(groupbyFields);
        }
        if (filterFields != null) {
            outputFields.addAll(filterFields);
        }
        if (sortFields != null) {
            outputFields.addAll(sortFields);
        }
        outputFields.addAll(aggregateFields);
        searchCondition.setOutputFields(outputFields);

        if (searchCondition.isOutputAll()) {
            LOG.info("Output: ALL");
        } else {
            LOG.info("Output: " + StringUtils.join(searchCondition.getOutputFields(), ", "));
        }

        if (!this.aggregateCondition.isTimeSeries()) {
            FlatAggregator agg = new FlatAggregator(groupbyFields, aggregateCondition.getAggregateFunctionTypes(), aggregateCondition.getAggregateFields());
            StreamReader reader = null;
            if (this.entityDef.getMetricDefinition() == null) {
                reader = new GenericEntityStreamReader(serviceName, searchCondition);
            } else { // metric aggregation need metric reader
                reader = new GenericMetricEntityDecompactionStreamReader(this.prefix, searchCondition);
            }
            reader.register(agg);
            reader.readAsStream();
            ArrayList<Map.Entry<List<String>, List<Double>>> obj = new ArrayList<>();
            obj.addAll(agg.result().entrySet());
            this.firstTimestamp = reader.getFirstTimestamp();
            this.lastTimestamp = reader.getLastTimestamp();
            if (this.sortOptions == null) {
                return (List<T>) obj;
            } else { // has sort options
                return (List<T>) PostFlatAggregateSort.sort(agg.result(), this.sortOptions, top);
            }
        } else {
            StreamReader reader;
            if (entityDef.getMetricDefinition() == null) {
                reader = new GenericEntityStreamReader(serviceName, searchCondition);
            } else {
                reader = new GenericMetricEntityDecompactionStreamReader(this.prefix, searchCondition);
            }
            TimeSeriesAggregator tsAgg = new TimeSeriesAggregator(groupbyFields,
                    aggregateCondition.getAggregateFunctionTypes(),
                    aggregateFields,
                    searchCondition.getStartTime(),
                    searchCondition.getEndTime(),
                    aggregateCondition.getIntervalMS());
            reader.register(tsAgg);

            // for sorting
            FlatAggregator sortAgg = null;
            if (sortOptions != null) {
                sortAgg = new FlatAggregator(groupbyFields, sortFuncs, sortFields);
                reader.register(sortAgg);
            }
            reader.readAsStream();
            ArrayList<Map.Entry<List<String>, List<double[]>>> obj = new ArrayList<Map.Entry<List<String>, List<double[]>>>();
            obj.addAll(tsAgg.getMetric().entrySet());

            this.firstTimestamp = reader.getFirstTimestamp();
            this.lastTimestamp = reader.getLastTimestamp();
            if (sortOptions == null) {
                return (List<T>) obj;
            } else { // has sort options
                return (List<T>) TimeSeriesPostFlatAggregateSort.sort(sortAgg.result(), tsAgg.getMetric(), this.sortOptions, top);
            }
        }
    }

    @Override
    public long getLastTimestamp() {
        return this.firstTimestamp;
    }

    @Override
    public long getFirstTimeStamp() {
        return this.lastTimestamp;
    }
}
