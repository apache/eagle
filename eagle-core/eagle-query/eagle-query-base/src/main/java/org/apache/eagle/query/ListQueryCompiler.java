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
package org.apache.eagle.query;

import org.apache.eagle.log.entity.filter.HBaseFilterBuilder;
import org.apache.eagle.log.entity.meta.EntityDefinition;
import org.apache.eagle.log.entity.meta.EntityDefinitionManager;
import org.apache.eagle.log.expression.ExpressionParser;
import org.apache.eagle.query.aggregate.AggregateFunctionType;
import org.apache.eagle.query.aggregate.AggregateFunctionTypeMatcher;
import org.apache.eagle.query.aggregate.timeseries.SortOption;
import org.apache.eagle.query.aggregate.timeseries.SortOptionsParser;
import org.apache.eagle.query.parser.EagleQueryParseException;
import org.apache.eagle.query.parser.EagleQueryParser;
import org.apache.eagle.query.parser.ORExpression;
import org.apache.eagle.query.parser.TokenConstant;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ListQueryCompiler {
    private static final Logger LOG = LoggerFactory.getLogger(ListQueryCompiler.class);
    /**
     * syntax is EntityName[Filter]{Projection}.
     */
    private static final String listRegex = "^([^\\[]+)\\[(.*)\\]\\{(.+)\\}$";
    private static final Pattern _listPattern = Pattern.compile(listRegex);

    /**
     * syntax is @fieldname.
     */
    private static final String _fnAnyPattern = "*";
    private static final Pattern _fnPattern = TokenConstant.ID_PATTERN;

    /**
     * syntax is @expression.
     */
    private static final String expRegex = "^(EXP\\{.*\\})(\\s+AS)?(\\s+.*)?$";
    private static final Pattern _expPattern = Pattern.compile(expRegex, Pattern.CASE_INSENSITIVE);

    /**
     * syntax is EntityName[Filter]GroupbyFields{AggregateFunctions}.
     */

    /**
     * The regular expression before add EXP{Expression} in query.
     **/
    private static final String aggRegex = "^([^\\[]+)\\[(.*)\\]<([^>]*)>\\{(.+)\\}$";
    private static final Pattern _aggPattern = Pattern.compile(aggRegex);

    private static final String sortRegex = "^([^\\[]+)\\[(.*)\\]<([^>]*)>\\{(.+)\\}\\.\\{(.+)\\}$";
    private static final Pattern _sortPattern = Pattern.compile(sortRegex);

    private String serviceName;
    private Filter filter;
    private List<String> outputFields;
    private List<String> groupbyFields;
    private List<AggregateFunctionType> aggregateFunctionTypes;
    private List<String> aggregateFields;
    private List<AggregateFunctionType> sortFunctionTypes;
    private List<String> sortFields;
    private Map<String, String> outputAlias;

    /**
     * Filed that must be required in filter.
     *
     * @return
     */
    public Set<String> getFilterFields() {
        return filterFields;
    }

    private Set<String> filterFields;
    private List<SortOption> sortOptions;
    private boolean hasAgg;
    private List<String[]> partitionValues;
    private boolean filterIfMissing;
    private ORExpression queryExpression;
    private boolean outputAll = false;

    public ListQueryCompiler(String query) throws Exception {
        this(query, false);
    }

    public ListQueryCompiler(String query, boolean filterIfMissing) throws Exception {
        this.filterIfMissing = filterIfMissing;
        Matcher m = _listPattern.matcher(query);
        if (m.find()) {
            if (m.groupCount() != 3) {
                throw new IllegalArgumentException("List query syntax is <EntityName>[<Filter>]{<Projection>}");
            }
            compileCollectionQuery(m);
            hasAgg = false;
            partitionConstraintValidate(query);
            return;
        }

        /** match sort pattern fist, otherwise some sort query will be mismatch as agg pattern */
        m = _sortPattern.matcher(query);
        if (m.find()) {
            if (m.groupCount() != 5) {
                throw new IllegalArgumentException("Aggregate query syntax is <EntityName>[<Filter>]<GroupbyFields>{<AggregateFunctions>}.{<SortOptions>}");
            }
            compileAggregateQuery(m);
            hasAgg = true;
            partitionConstraintValidate(query);
            return;
        }

        m = _aggPattern.matcher(query);
        if (m.find()) {
            //if(m.groupCount() < 4 || m.groupCount() > 5)
            if (m.groupCount() != 4) {
                throw new IllegalArgumentException("Aggregate query syntax is <EntityName>[<Filter>]<GroupbyFields>{<AggregateFunctions>}.{<SortOptions>}");
            }
            compileAggregateQuery(m);
            hasAgg = true;
            partitionConstraintValidate(query);
            return;
        }

        throw new IllegalArgumentException("List query syntax is <EntityName>[<Filter>]{<Projection>} \n Aggregate query syntax is <EntityName>[<Filter>]<GroupbyFields>{<AggregateFunctions>}"
            + ".{<SortOptions>}");
    }

    /**
     * TODO: For now we don't support one query to query multiple partitions. In future if partition is defined
     * for the entity, internally We need to spawn multiple queries and send one query for each search condition
     * for each partition.
     *
     * @param query input query to compile
     */
    private void partitionConstraintValidate(String query) {
        if (partitionValues != null && partitionValues.size() > 1) {
            final String[] values = partitionValues.get(0);
            for (int i = 1; i < partitionValues.size(); ++i) {
                final String[] tmpValues = partitionValues.get(i);
                for (int j = 0; j < values.length; ++j) {
                    if (values[j] == null || (!values[j].equals(tmpValues[j]))) {
                        final String errMsg = "One query for multiple partitions is NOT allowed for now! Query: " + query;
                        LOG.error(errMsg);
                        throw new IllegalArgumentException(errMsg);
                    }
                }
            }
        }
    }

    public boolean hasAgg() {
        return hasAgg;
    }

    public List<String[]> getQueryPartitionValues() {
        return partitionValues;
    }

    public ORExpression getQueryExpression() {
        return queryExpression;
    }

    private void checkEntityExistence(String entityName) throws EagleQueryParseException {
        try {
            if (EntityDefinitionManager.getEntityByServiceName(entityName) == null) {
                throw new EagleQueryParseException(entityName + " entity does not exist!");
            }
        } catch (InstantiationException e) {
            final String errMsg = "Got an InstantiationException: " + e.getMessage();
            throw new EagleQueryParseException(entityName + " entity does not exist! " + errMsg);
        } catch (IllegalAccessException e) {
            final String errMsg = "Got an IllegalAccessException: " + e.getMessage();
            throw new EagleQueryParseException(entityName + " entity does not exist! " + errMsg);
        }
    }

    public String deleteAtSign(String expression) {
        return expression.replace("@", "");
    }

    private void compileCollectionQuery(Matcher m) throws EagleQueryParseException {
        serviceName = m.group(1);
        checkEntityExistence(serviceName);
        if (outputFields == null) {
            outputFields = new ArrayList<String>();
        }
        String qy = m.group(2);
        filter = compileQy(qy);
        String prjFields = m.group(3);
        String[] tmp = prjFields.split(",");
        for (String str : tmp) {
            str = str.trim();
            Matcher fnMatcher = _fnPattern.matcher(str);
            Matcher expMatcher = _expPattern.matcher(str);
            if (fnMatcher.find()) {
                if (fnMatcher.groupCount() == 1) {
                    outputFields.add(fnMatcher.group(1));
                }
            } else if (_fnAnyPattern.equals(str)) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Output all fields");
                }
                // _outputFields.add(_fnAnyPattern);
                this.outputAll = true;
            } else if (expMatcher.find()) {
                String expr = deleteAtSign(expMatcher.group(1));
                String alias = expMatcher.group(3);
                try {
                    String exprContent = TokenConstant.parseExpressionContent(expr);
                    outputFields.addAll(ExpressionParser.parse(exprContent).getDependentFields());
                    if (alias != null) {
                        if (outputAlias == null) {
                            outputAlias = new HashMap<String, String>();
                        }
                        outputAlias.put(exprContent, alias.trim());
                    }
                } catch (Exception ex) {
                    LOG.error("Failed to parse expression: " + expr + ", exception: " + ex.getMessage(), ex);
                } finally {
                    outputFields.add(expr);
                }
            } else {
                throw new IllegalArgumentException("Field name syntax must be @<FieldName> or * or Expression in syntax EXP{<Expression>}");
            }
        }
    }

    private void compileAggregateQuery(Matcher m) throws EagleQueryParseException {
        serviceName = m.group(1);
        checkEntityExistence(serviceName);
        String qy = m.group(2);
        filter = compileQy(qy);
        String groupbyFields = m.group(3);
        // groupbyFields could be empty
        List<String> groupbyFieldList = null;
        this.groupbyFields = new ArrayList<String>();
        if (!groupbyFields.isEmpty()) {
            groupbyFieldList = Arrays.asList(groupbyFields.split(","));
            for (String str : groupbyFieldList) {
                Matcher fnMatcher = _fnPattern.matcher(str.trim());
                if (!fnMatcher.find() || fnMatcher.groupCount() != 1) {
                    throw new IllegalArgumentException("Field name syntax must be @<FieldName>");
                }
                this.groupbyFields.add(fnMatcher.group(1));
            }
        }
        String functions = m.group(4);
        // functions
        List<String> functionList = Arrays.asList(functions.split(","));
        aggregateFunctionTypes = new ArrayList<AggregateFunctionType>();
        aggregateFields = new ArrayList<String>();
        for (String function : functionList) {
            AggregateFunctionTypeMatcher matcher = AggregateFunctionType.matchAll(function.trim());
            if (!matcher.find()) {
                throw new IllegalArgumentException("Aggregate function must have format of count|sum|avg|max|min(<fieldname|expression>)");
            }
            aggregateFunctionTypes.add(matcher.type());
            String aggField = deleteAtSign(matcher.field().trim());
            try {
                if (outputFields == null) {
                    outputFields = new ArrayList<String>();
                }
                if (TokenConstant.isExpression(aggField)) {
                    outputFields.addAll(ExpressionParser.parse(TokenConstant.parseExpressionContent(aggField)).getDependentFields());
                } else {
                    outputFields.add(aggField);
                }
            } catch (Exception ex) {
                LOG.error("Failed to parse expression: " + aggField + ", exception: " + ex.getMessage(), ex);
            } finally {
                aggregateFields.add(aggField);
            }
        }

        // sort options
        if (m.groupCount() < 5 || m.group(5) == null) { // no sort options
            return;
        }
        String sortOptions = m.group(5);
        if (sortOptions != null) {
            LOG.info("SortOptions: " + sortOptions);
            List<String> sortOptionList = Arrays.asList(sortOptions.split(","));
            List<String> rawSortFields = new ArrayList<String>();
            this.sortOptions = SortOptionsParser.parse(groupbyFieldList, functionList, sortOptionList, rawSortFields);
            this.sortFunctionTypes = new ArrayList<>();
            this.sortFields = new ArrayList<>();
            for (String sortField : rawSortFields) {
                AggregateFunctionTypeMatcher matcher = AggregateFunctionType.matchAll(sortField);
                if (matcher.find()) {
                    sortFunctionTypes.add(matcher.type());
                    sortFields.add(deleteAtSign(matcher.field().trim()));
                }
            }
        }
    }

    /**
     * 1. syntax level - use antlr to pass the queries
     * 2. semantics level - can't distinguish tag or qualifier
     *
     * @param qy
     * @return
     */
    private Filter compileQy(String qy) throws EagleQueryParseException {
        try {
            EntityDefinition ed = EntityDefinitionManager.getEntityByServiceName(serviceName);
            if (qy == null || qy.isEmpty()) {
                if (ed.getPartitions() == null) {
                    if (LOG.isDebugEnabled()) {
                        LOG.warn("Query string is empty, full table scan query: " + qy);
                    }
                    // For hbase 0.98+, empty FilterList() will filter all rows, so we need return null instead
                    return null;
                } else {
                    final String errMsg = "Entity " + ed.getEntityClass().getSimpleName() + " defined partition, "
                        + "but query doesn't provide partition condition! Query: " + qy;
                    LOG.error(errMsg);
                    throw new IllegalArgumentException(errMsg);
                }
            }
            EagleQueryParser parser = new EagleQueryParser(qy);
            queryExpression = parser.parse();

            //TODO: build customize filter for EXP{<Expression>}
            HBaseFilterBuilder builder = new HBaseFilterBuilder(ed, queryExpression, filterIfMissing);
            FilterList flist = builder.buildFilters();
            partitionValues = builder.getPartitionValues();
            filterFields = builder.getFilterFields();
            return flist;
        } catch (InstantiationException e) {
            final String errMsg = "Got an InstantiationException: " + e.getMessage();
            throw new EagleQueryParseException(serviceName + " entity does not exist! " + errMsg);
        } catch (IllegalAccessException e) {
            final String errMsg = "Got an IllegalAccessException: " + e.getMessage();
            throw new EagleQueryParseException(serviceName + " entity does not exist! " + errMsg);
        }
    }

    public String serviceName() {
        return serviceName;
    }

    public List<String> outputFields() {
        return outputFields;
    }

    public Filter filter() {
        return filter;
    }

    public List<String> groupbyFields() {
        return groupbyFields;
    }

    public List<AggregateFunctionType> aggregateFunctionTypes() {
        return aggregateFunctionTypes;
    }

    public List<String> aggregateFields() {
        return aggregateFields;
    }

    public List<SortOption> sortOptions() {
        return sortOptions;
    }

    public List<AggregateFunctionType> sortFunctions() {
        return sortFunctionTypes;
    }

    public List<String> sortFields() {
        return sortFields;
    }

    /**
     * Output all fields (i.e. has * in out fields)
     *
     * @return
     */
    public boolean isOutputAll() {
        return outputAll;
    }

    public Map<String, String> getOutputAlias() {
        return outputAlias;
    }
}
