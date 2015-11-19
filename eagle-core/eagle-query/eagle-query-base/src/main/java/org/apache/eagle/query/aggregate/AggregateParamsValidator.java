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
package org.apache.eagle.query.aggregate;

import java.util.ArrayList;
import java.util.List;

public class AggregateParamsValidator {
	/**
	 * This method handle the following sytle syntax
	 * sum(numConfiguredMapSlots), count group by cluster, rack 
	 * 1. ensure that all gb fields must occur in outputField or outputTag
	 * 2. ensure that all summarized fields must occur in outputField, 
	 *    for example, for function=sum(numConfiguredMapSlots), numConfiguredMapSlots must occur in outputField  
	 * 3. groupby should be pre-appended with a root groupby field  
	 * @param outputTags
	 * @param outputFields
	 * @param groupbys
	 * @param functions
	 * @throws IllegalArgumentException
	 */
	public static AggregateParams compileAggregateParams(List<String> outputTags, List<String> outputFields, List<String> groupbys, List<String> functions, List<String> sortFieldOrders)
			throws IllegalArgumentException, AggregateFunctionNotSupportedException{
		AggregateParams aggParams = new AggregateParams();
		// ensure that all gb fields must occur in outputField or outputTag
		for(String groupby : groupbys){
			if(!outputTags.contains(groupby) && !outputFields.contains(groupby)){
				throw new IllegalArgumentException(groupby + ", All gb fields should appear in outputField list or outputTag list");
			}
		}
		
		// parse functions and ensure that all summarized fields must occur in outputField
		for(String function : functions){
			AggregateFunctionTypeMatcher m = AggregateFunctionType.count.matcher(function);
			if(m.find()){
				aggParams.setCounting(true);
				continue;
			}

			m = AggregateFunctionType.sum.matcher(function);
			if(m.find()){
				if(!outputFields.contains(m.field())){
					throw new IllegalArgumentException(m.field() + ", All summary function fields should appear in outputField list");
				}
				aggParams.getSumFunctionFields().add(m.field());
				continue;
			}
			
			throw new AggregateFunctionNotSupportedException("function " + function + " is not supported, only count, sum aggregate functions are now supported");
		}
		
		//  groupby should be pre-appended with a root groupby field
		List<String> groupbyFields = new ArrayList<String>();
		groupbyFields.add(Aggregator.GROUPBY_ROOT_FIELD_NAME);
		groupbyFields.addAll(groupbys);
		aggParams.setGroupbyFields(groupbyFields);

		// check sort field orders
		boolean byKeySorting = false;
		for(String sortFieldOrder : sortFieldOrders){
			AggregateParams.SortFieldOrder sfo = SortFieldOrderType.matchAll(sortFieldOrder);
			if(sfo == null){
				throw new IllegalArgumentException(sortFieldOrder + ", All sort field order should be <field>=(asc|desc)");
			}
			if(sfo.getField().equals(AggregateParams.SortFieldOrder.SORT_BY_AGGREGATE_KEY)){
				byKeySorting =  true;
			}else if(!sfo.getField().equals(AggregateParams.SortFieldOrder.SORT_BY_COUNT)){
				if(!groupbys.contains(sfo.getField()) && !aggParams.getSumFunctionFields().contains(sfo.getField())){
					throw new IllegalArgumentException(sortFieldOrder + ", All sort field order should appear in gb or function fields");
				}
			}
			aggParams.getSortFieldOrders().add(sfo);
		}
		// always add key ascendant to the last aggregation key if not specified
		if(!byKeySorting){
			aggParams.getSortFieldOrders().add(new AggregateParams.SortFieldOrder(AggregateParams.SortFieldOrder.SORT_BY_AGGREGATE_KEY, true));
		}
		return aggParams;
	}
}
