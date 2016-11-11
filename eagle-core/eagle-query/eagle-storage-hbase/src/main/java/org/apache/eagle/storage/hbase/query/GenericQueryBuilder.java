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
package org.apache.eagle.storage.hbase.query;


import org.apache.eagle.common.config.EagleConfigFactory;
import org.apache.eagle.log.entity.SearchCondition;
import org.apache.eagle.query.GenericEntityQuery;
import org.apache.eagle.query.GenericQuery;
import org.apache.eagle.query.aggregate.AggregateCondition;
import org.apache.eagle.query.aggregate.AggregateFunctionType;
import org.apache.eagle.query.aggregate.timeseries.SortOption;
import org.apache.eagle.storage.hbase.query.aggregate.GenericCoprocessorAggregateQuery;
import org.apache.eagle.storage.hbase.query.aggregate.GenericInMemoryAggregateQuery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class GenericQueryBuilder {
    private static final Logger LOG = LoggerFactory.getLogger(GenericQueryBuilder.class);

    private List<String> outputFields;
    private String serviceName;
    private String metricName;
    private SearchCondition searchCondition;
    @Deprecated
    private int parallel;
    private boolean hasAgg;
    private List<String> groupByFields;
    private List<AggregateFunctionType> aggregateFuncTypes;
    private List<String> aggregateFields;
    @Deprecated
    private boolean treeAgg = false;
    private boolean timeSeries = false;
    private long intervalmin;
    private List<SortOption> sortOptions;
    private int top;
    private List<AggregateFunctionType> sortFunctionTypes;
    private List<String> sortFields;

    public static GenericQueryBuilder select(List<String> outputFields) {
        GenericQueryBuilder builder = new GenericQueryBuilder();
        builder.output(outputFields);
        return builder;
    }

    public GenericQueryBuilder output(List<String> outputFields) {
        this.outputFields = outputFields;
        return this;
    }

    public GenericQueryBuilder from(String serviceName, String metricName) {
        this.serviceName = serviceName;
        this.metricName = metricName;
        return this;
    }

    public GenericQueryBuilder where(SearchCondition condition) {
        this.searchCondition = condition;
        return this;
    }

    /**
     * @deprecated Parameter "parallel" no longer supported, ignore.
     */
    @Deprecated
    public GenericQueryBuilder parallel(int parallel) {
        if (parallel > 0) {
            LOG.warn("Parameter \"parallel\" is deprecated, ignore");
        }
        return this;
    }

    public GenericQueryBuilder groupBy(boolean hasAgg, List<String> groupByFields, List<AggregateFunctionType> aggregateFunctionTypes, List<String> aggregateFields) {
        this.hasAgg = hasAgg;
        this.groupByFields = groupByFields;
        this.aggregateFuncTypes = aggregateFunctionTypes;
        this.aggregateFields = aggregateFields;
        return this;
    }

    public GenericQueryBuilder timeSeries(boolean timeSeries, long intervalMin) {
        this.timeSeries = timeSeries;
        this.intervalmin = intervalMin;
        return this;
    }

    public GenericQueryBuilder orderBy(List<SortOption> sortOptions, List<AggregateFunctionType> sortFunctionTypes, List<String> sortFields) {
        this.sortOptions = sortOptions;
        this.sortFunctionTypes = sortFunctionTypes;
        this.sortFields = sortFields;
        return this;
    }

    public GenericQueryBuilder top(int top) {
        this.top = top;
        return this;
    }

    /**
     * @deprecated Parameter "treeAgg" no longer supported, ignore.
     */
    @Deprecated
    public GenericQueryBuilder treeAgg(boolean treeAgg) {
        if (treeAgg) {
            LOG.warn("Parameter \"treeAgg\" is deprecated, ignore");
        }
        return this;
    }

    public GenericQuery build() throws Exception {
        if (hasAgg) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Build GroupAggregateQuery");
            }
            AggregateCondition aggregateCondition = new AggregateCondition();
            aggregateCondition.setGroupbyFields(this.groupByFields);
            aggregateCondition.setAggregateFunctionTypes(this.aggregateFuncTypes);
            aggregateCondition.setAggregateFields(this.aggregateFields);
            aggregateCondition.setTimeSeries(this.timeSeries);
            aggregateCondition.setIntervalMS(this.intervalmin * 60 * 1000);
            if (EagleConfigFactory.load().isCoprocessorEnabled()) {
                return new GenericCoprocessorAggregateQuery(this.serviceName,
                    this.searchCondition,
                    aggregateCondition,
                    this.metricName,
                    this.sortOptions,
                    this.sortFunctionTypes,
                    this.sortFields,
                    this.top);
            } else {
                return new GenericInMemoryAggregateQuery(this.serviceName,
                    this.searchCondition,
                    aggregateCondition,
                    this.metricName,
                    this.sortOptions,
                    this.sortFunctionTypes,
                    this.sortFields,
                    this.top);
            }
        } else {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Build GenericBatchQuery");
            }
            return new GenericEntityQuery(this.serviceName, this.searchCondition, this.metricName);
        }
    }
}
