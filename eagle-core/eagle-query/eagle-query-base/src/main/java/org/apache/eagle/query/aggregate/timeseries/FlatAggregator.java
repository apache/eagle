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
package org.apache.eagle.query.aggregate.timeseries;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.eagle.log.base.taggedlog.TaggedLogAPIEntity;
import org.apache.eagle.query.aggregate.AggregateFunctionType;

/**
 * Not thread safe
 */
public class FlatAggregator extends AbstractAggregator{
	protected GroupbyBucket bucket;

    /**
     * @param groupbyFields
     * @param aggregateFuntionTypes
     * @param aggregatedFields
     */
	public FlatAggregator(List<String> groupbyFields, List<AggregateFunctionType> aggregateFuntionTypes, List<String> aggregatedFields){
		super(groupbyFields, aggregateFuntionTypes, aggregatedFields);
		bucket = new GroupbyBucket(this.aggregateFunctionTypes);
	}
	
	public void accumulate(TaggedLogAPIEntity entity) throws Exception{
		List<String> groupbyFieldValues = createGroup(entity);
		List<Double> preAggregatedValues = createPreAggregatedValues(entity);
		bucket.addDatapoint(groupbyFieldValues, preAggregatedValues);
	}
	
	public Map<List<String>, List<Double>> result(){
		return bucket.result(); 
	}
	
	protected List<String> createGroup(TaggedLogAPIEntity entity){
		List<String> groupbyFieldValues = new ArrayList<String>();
		int i = 0;
		for(String groupbyField : groupbyFields){
			String groupbyFieldValue = determineGroupbyFieldValue(entity, groupbyField, i++);
			groupbyFieldValues.add(groupbyFieldValue);
		}
		return groupbyFieldValues;
	}
}
