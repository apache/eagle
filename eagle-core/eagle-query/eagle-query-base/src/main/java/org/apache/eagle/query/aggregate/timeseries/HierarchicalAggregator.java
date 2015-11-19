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

import java.util.List;
import java.util.SortedMap;

import org.apache.eagle.log.base.taggedlog.TaggedLogAPIEntity;
import org.apache.eagle.query.aggregate.AggregateFunctionType;

public class HierarchicalAggregator extends AbstractAggregator{
	private HierarchicalAggregateEntity root = new HierarchicalAggregateEntity();

	public HierarchicalAggregator(List<String> groupbyFields, List<AggregateFunctionType> aggregateFuntionTypes, List<String> aggregatedFields){
		super(groupbyFields, aggregateFuntionTypes, aggregatedFields);
	}

	public void accumulate(TaggedLogAPIEntity entity) throws Exception{
		List<Double> preAggregatedValues = createPreAggregatedValues(entity);
		// aggregate to root first
		addDatapoint(root, preAggregatedValues);
		// go through hierarchical tree
		HierarchicalAggregateEntity current = root;
		int i = 0;
		for(String groupbyField : groupbyFields){
			// determine groupbyFieldValue from tag or fields
			String groupbyFieldValue = determineGroupbyFieldValue(entity, groupbyField, i);
			SortedMap<String, HierarchicalAggregateEntity> children = current.getChildren();
			if(children.get(groupbyFieldValue) == null){
				HierarchicalAggregateEntity tmp = new HierarchicalAggregateEntity();
				children.put(groupbyFieldValue, tmp);
			}
			children.get(groupbyFieldValue).setKey(groupbyFieldValue);
			addDatapoint(children.get(groupbyFieldValue), preAggregatedValues);
			current = children.get(groupbyFieldValue);
		}
	}

	private void addDatapoint(HierarchicalAggregateEntity entity, List<Double> values){
		List<GroupbyBucket.Function> functions = entity.getTmpValues();
		// initialize list of function
		if(functions.isEmpty()){
			for(AggregateFunctionType type : aggregateFunctionTypes){
				functions.add(GroupbyBucket._functionFactories.get(type.name()).createFunction());
			}
		}
		int functionIndex = 0;
		for(Double v : values){
			functions.get(functionIndex).run(v);
			functionIndex++;
		}
	}

	private void finalizeHierarchicalAggregateEntity(HierarchicalAggregateEntity entity){
		for(GroupbyBucket.Function f : entity.getTmpValues()){
			entity.getValues().add(f.result());
		}
		for(HierarchicalAggregateEntity child : entity.getChildren().values()){
			finalizeHierarchicalAggregateEntity(child);
		}
		entity.setTmpValues(null);
	}

	public HierarchicalAggregateEntity result(){
		finalizeHierarchicalAggregateEntity(root);
		return this.root;
	}
}
