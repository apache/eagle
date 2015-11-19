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
package org.apache.eagle.storage.hbase.query.coprocessor;

import org.apache.eagle.log.entity.meta.EntityDefinition;
import org.apache.eagle.query.aggregate.AggregateFunctionType;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Scan;

import java.io.IOException;
import java.util.List;

/**
 * Coprocessor-based Aggregation Universal Client Interface
 *
 * <h2>Flat or RAW Aggregation:</h2>
 * <pre>
 * AggregateResult aggregate( HTableInterface table, String serviceName, Scan scan, List<String> groupbyFields, List<AggregateFunctionType> aggregateFuncTypes, List<String> aggregatedFields) throws IOException
 * </pre>
 *
 * <h2>Time Series Aggregation:</h2>
 * <pre>
 * AggregateResult aggregate(HTableInterface table, String serviceName, Scan scan, List<String> groupbyFields, List<AggregateFunctionType> aggregateFuncTypes, List<String> aggregatedFields, boolean timeSeries, long startTime, long endTime, long intervalMin) throws IOException
 * </pre>
 * @since : 11/3/14,2014
 */
public interface AggregateClient
{

	/**
	 * Flat Aggregation
	 *
	 *
	 * @param table                   HTable connections
	 * @param scan                    HBase Scan
	 * @param groupbyFields           Grouped by fields name
	 * @param aggregateFuncTypes      Aggregate function types
	 * @param aggregatedFields        Aggregate field names
	 * @return                        Return AggregateResult
	 * @throws Exception
	 */
	AggregateResult aggregate(final HTableInterface table,                            // HTable connections
                              final EntityDefinition entityDefinition,                               // Eagle service name
                              final Scan scan,                                        // HBase Scan
                              final List<String> groupbyFields,                       // Grouped by fields name
                              final List<AggregateFunctionType> aggregateFuncTypes,   // Aggregate function types
                              final List<String> aggregatedFields                     // Aggregate field names
    ) throws IOException;

	/**
	 * Time Series Aggregation
	 *
	 * @param table                   HTable connections
	 * @param entityDefinition        Eagle EntityDefinition
	 * @param scan                    HBase Scan
	 * @param groupbyFields           Grouped by fields name
	 * @param aggregateFuncTypes      Aggregate function types
	 * @param aggregatedFields        Aggregate field names
	 * @param timeSeries              Is time series aggregations?
	 * @param intervalMin             The interval in minutes if it's time series aggregation
	 * @return                        Return AggregateResult
	 * @throws Exception
	 */
	AggregateResult aggregate(final HTableInterface table,                                // HTable connections
                              final EntityDefinition entityDefinition,                               // Eagle service name
                              final Scan scan,                                        // HBase Scan
                              final List<String> groupbyFields,                       // Grouped by fields name
                              final List<AggregateFunctionType> aggregateFuncTypes,   // Aggregate function types
                              final List<String> aggregatedFields,                    // Aggregate field names
                              final boolean timeSeries,                               // Is time series aggregations?
                              final long startTime,                                   // startTime
                              final long endTime,                                     // endTime
                              final long intervalMin                                   // The interval in minutes if it's time series aggregation
    ) throws IOException;
}