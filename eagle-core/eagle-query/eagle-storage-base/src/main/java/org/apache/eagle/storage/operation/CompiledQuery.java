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
package org.apache.eagle.storage.operation;

import org.apache.eagle.log.entity.GenericMetricEntity;
import org.apache.eagle.log.entity.SearchCondition;
import org.apache.eagle.log.entity.meta.EntityDefinition;
import org.apache.eagle.log.entity.meta.EntityDefinitionManager;
import org.apache.eagle.query.ListQueryCompiler;
import org.apache.eagle.query.aggregate.timeseries.SortOption;
import org.apache.eagle.query.aggregate.AggregateFunctionType;
import org.apache.eagle.storage.exception.QueryCompileException;
import org.apache.eagle.common.DateTimeUtil;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

/**
 * TODO: refactor to remove hbase related fields
 *
 * @since 3/23/15
 */
public class CompiledQuery {
    private final static Logger LOG = LoggerFactory.getLogger(CompiledQuery.class);

    public boolean isHasAgg() {
        return hasAgg;
    }

    public void setHasAgg(boolean hasAgg) {
        this.hasAgg = hasAgg;
    }

    public List<AggregateFunctionType> getAggregateFunctionTypes() {
        return aggregateFunctionTypes;
    }

    public void setAggregateFunctionTypes(List<AggregateFunctionType> aggregateFunctionTypes) {
        this.aggregateFunctionTypes = aggregateFunctionTypes;
    }

    public List<SortOption> getSortOptions() {
        return sortOptions;
    }

    public void setSortOptions(List<SortOption> sortOptions) {
        this.sortOptions = sortOptions;
    }

    public List<AggregateFunctionType> getSortFunctions() {
        return sortFunctions;
    }

    public void setSortFunctions(List<AggregateFunctionType> sortFunctions) {
        this.sortFunctions = sortFunctions;
    }

    public List<String> getSortFields() {
        return sortFields;
    }

    public void setSortFields(List<String> sortFields) {
        this.sortFields = sortFields;
    }

    public static Logger getLog() {

        return LOG;
    }

    public String getServiceName() {
        return serviceName;
    }

    public void setServiceName(String serviceName) {
        this.serviceName = serviceName;
    }

    public SearchCondition getSearchCondition() {
        return searchCondition;
    }

    public void setSearchCondition(SearchCondition searchCondition) {
        this.searchCondition = searchCondition;
    }

    public RawQuery getRawQuery() {
        return rawQuery;
    }

    private String serviceName;
    private SearchCondition searchCondition;
    private final RawQuery rawQuery;
    private boolean hasAgg;
    private List<AggregateFunctionType> aggregateFunctionTypes;
    private List<SortOption> sortOptions;
    private List<AggregateFunctionType> sortFunctions;
    private List<String> sortFields;
    private List<String> groupByFields;

    private long startTime;

    public long getEndTime() {
        return endTime;
    }

    public void setEndTime(long endTime) {
        this.endTime = endTime;
    }

    public long getStartTime() {
        return startTime;
    }

    public void setStartTime(long startTime) {
        this.startTime = startTime;
    }

    private long endTime;

    public List<String> getAggregateFields() {
        return aggregateFields;
    }

    public void setAggregateFields(List<String> aggregateFields) {
        this.aggregateFields = aggregateFields;
    }

    private List<String> aggregateFields;

    public CompiledQuery(RawQuery rawQueryCondition) throws QueryCompileException {
        this.rawQuery = rawQueryCondition;
        try {
            this.compile();
        } catch (Exception e) {
            throw new QueryCompileException(e);
        }
    }

    public List<String> getGroupByFields() {
        return groupByFields;
    }

    public void setGroupByFields(List<String> groupByFields) {
        this.groupByFields = groupByFields;
    }

    private void validateQueryParameters(String startRowkey, int pageSize){
        if(pageSize < 0){
            throw new IllegalArgumentException("Positive pageSize value should be always provided. The list query format is:\n" + "eagle-service/rest/list?query=<querystring>&pageSize=10&startRowkey=xyz&startTime=xxx&endTime=xxx");
        }

        if(startRowkey != null && startRowkey.equals("null")){
            LOG.warn("startRowkey being null string is not same to startRowkey == null");
        }
        return;
    }

    private void checkNotNull(Object obj,String name){
        if(obj == null) throw new IllegalArgumentException(name+" should not be null");
    }

    protected void compile() throws Exception {
        validateQueryParameters(this.getRawQuery().getStartRowkey(),this.getRawQuery().getPageSize());
        checkNotNull(this.rawQuery,"rawQuery instance");
        checkNotNull(this.rawQuery.getQuery(),"query");

        ListQueryCompiler compiler = new ListQueryCompiler(this.rawQuery.getQuery(),this.rawQuery.isFilterIfMissing());
        this.serviceName = compiler.serviceName();
        this.searchCondition = new SearchCondition();
        this.searchCondition.setOutputVerbose(this.rawQuery.isVerbose() );
        this.searchCondition.setOutputAlias(compiler.getOutputAlias());
        this.searchCondition.setFilter(compiler.filter());
        this.searchCondition.setQueryExpression(compiler.getQueryExpression());
        if(compiler.sortOptions() == null && this.rawQuery.getTop() > 0) {
            LOG.warn("Parameter \"top\" is only used for sort query! Ignore top parameter this time since it's not a sort query");
        }

        this.hasAgg = compiler.hasAgg();
        this.aggregateFunctionTypes = compiler.aggregateFunctionTypes();
        this.sortOptions = compiler.sortOptions();
        this.sortFields = compiler.sortFields();
        this.sortFunctions = compiler.sortFunctions();
        this.groupByFields = compiler.groupbyFields();
        this.aggregateFields = compiler.aggregateFields();

        final List<String[]> partitionValues = compiler.getQueryPartitionValues();
        if (partitionValues != null) {
            this.searchCondition.setPartitionValues(Arrays.asList(partitionValues.get(0)));
        }

        // 3. Set time range if it's timeseries service
        EntityDefinition ed = EntityDefinitionManager.getEntityByServiceName(serviceName);
        if(ed.isTimeSeries()){
            // TODO check Time exists for timeseries or topology data
            this.searchCondition.setStartTime(this.rawQuery.getStartTime());
            this.searchCondition.setEndTime(this.rawQuery.getEndTime());
            this.setStartTime(DateTimeUtil.humanDateToSeconds(this.getRawQuery().getStartTime()) * 1000);
            this.setEndTime(DateTimeUtil.humanDateToSeconds(this.getRawQuery().getEndTime()) * 1000);
        }

        // 4. Set HBase start scanning rowkey if given
        searchCondition.setStartRowkey(this.rawQuery.getStartRowkey());

        // 5. Set page size
        searchCondition.setPageSize(this.rawQuery.getPageSize());

        // 6. Generate output,group-by,aggregated fields
        List<String> outputFields = compiler.outputFields();
        List<String> groupbyFields = compiler.groupbyFields();
        List<String> aggregateFields = compiler.aggregateFields();
        Set<String> filterFields = compiler.getFilterFields();

        // Start to generate output fields list {
        searchCondition.setOutputAll(compiler.isOutputAll());
        if(outputFields == null) outputFields = new ArrayList<String>();
        if(compiler.hasAgg()){
            if(groupbyFields != null) outputFields.addAll(groupbyFields);
            if(aggregateFields != null) outputFields.addAll(aggregateFields);
            if(GenericMetricEntity.GENERIC_METRIC_SERVICE.equals(serviceName) && !outputFields.contains(GenericMetricEntity.VALUE_FIELD)){
                outputFields.add(GenericMetricEntity.VALUE_FIELD);
            }
        }
        if(filterFields!=null) outputFields.addAll(filterFields);
        searchCondition.setOutputFields(outputFields);
        if(LOG.isDebugEnabled()) {
            if (compiler.isOutputAll()) {
                LOG.debug("Output fields: all");
            } else {
                LOG.debug("Output fields: " + StringUtils.join(outputFields, ","));
            }
        }
    }
}