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
package org.apache.eagle.storage.jdbc.criteria.impl;

import org.apache.eagle.log.entity.GenericMetricEntity;
import org.apache.eagle.log.entity.SearchCondition;
import org.apache.eagle.query.aggregate.AggregateFunctionType;
import org.apache.eagle.query.parser.ORExpression;
import org.apache.eagle.storage.jdbc.criteria.CriteriaBuilder;
import org.apache.eagle.storage.jdbc.criteria.CriterionBuilder;
import org.apache.eagle.storage.jdbc.JdbcConstants;
import org.apache.eagle.storage.operation.CompiledQuery;
import org.apache.torque.ColumnImpl;
import org.apache.torque.criteria.Criteria;
import org.apache.torque.criteria.Criterion;
import org.apache.torque.criteria.SqlEnum;

import java.util.List;

/**
 * @since 3/27/15
 */
public class QueryCriteriaBuilder implements CriteriaBuilder {

    private final CompiledQuery query;
    private final String tableName;

    public QueryCriteriaBuilder(CompiledQuery query, String tableName){
        this.query = query;
        this.tableName = tableName;
    }

    @Override
    public Criteria build() {
        Criteria root = new Criteria();
        SearchCondition searchCondition = query.getSearchCondition();

        // SELECT
        if(query.isHasAgg()){
            List<String> aggFields = query.getAggregateFields();
            List<AggregateFunctionType> aggFuncs = query.getAggregateFunctionTypes();
            for(int i=0;i<aggFuncs.size();i++){
                AggregateFunctionType aggFunc = aggFuncs.get(i);
                String aggField = aggFields.get(i);
                if(aggFunc.equals(AggregateFunctionType.count)){
                    root.addSelectColumn(new ColumnImpl(aggFunc.name()+"(*)"));
                }else{
                    root.addSelectColumn(new ColumnImpl(String.format("%s(%s.%s)",aggFunc.name(),this.tableName,aggField)));
                }
            }
        } else if(searchCondition.isOutputAll()){
            // SELECT *
            root.addSelectColumn(new ColumnImpl(this.tableName, "*"));
        }else{
            // SELECT $outputFields
            List<String> outputFields = searchCondition.getOutputFields();
            for(String field:outputFields) {
                root.addSelectColumn(new ColumnImpl(this.tableName, field));
            }
            if(!outputFields.contains(JdbcConstants.ROW_KEY_COLUMN_NAME)) {
                root.addSelectColumn(new ColumnImpl(this.tableName, JdbcConstants.ROW_KEY_COLUMN_NAME));
            }
            if(!outputFields.contains(JdbcConstants.TIMESTAMP_COLUMN_NAME)) {
                root.addSelectColumn(new ColumnImpl(this.tableName, JdbcConstants.TIMESTAMP_COLUMN_NAME));
            }
        }

        // FROM $tableName
        root.addFrom(this.tableName);

        // WHERE timestamp in time range
        Criterion where = new Criterion(new ColumnImpl(this.tableName, JdbcConstants.TIMESTAMP_COLUMN_NAME),query.getStartTime(), SqlEnum.GREATER_EQUAL)
                        .and(new Criterion(new ColumnImpl(this.tableName, JdbcConstants.TIMESTAMP_COLUMN_NAME),query.getEndTime(), SqlEnum.LESS_THAN));
        ORExpression expression = searchCondition.getQueryExpression();
        if(expression!=null){
            CriterionBuilder criterionBuilder = new ExpressionCriterionBuilder(expression,tableName);
            where = where.and(criterionBuilder.build());
        }

        // AND metricName = "metric.name.value"
        if(query.getServiceName().equals(GenericMetricEntity.GENERIC_METRIC_SERVICE)){
            where = where.and(new Criterion(new ColumnImpl(this.tableName, JdbcConstants.METRIC_NAME_COLUMN_NAME),this.query.getRawQuery().getMetricName(),SqlEnum.EQUAL));
        }

        root.where(where);

        // LIMITED BY $pageSize
        root.setLimit((int) searchCondition.getPageSize());

        // TODO: GROUP BY
        if(query.isHasAgg()){
            for(String groupByField:query.getGroupByFields()){
                root.addGroupByColumn(new ColumnImpl(this.tableName,groupByField));
            }
        }

        // TODO: ORDER BY

        return root;
    }
}
