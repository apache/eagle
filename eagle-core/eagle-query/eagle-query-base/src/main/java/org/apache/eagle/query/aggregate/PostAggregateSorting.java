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

import java.lang.reflect.Method;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PostAggregateSorting {
	private static final Logger LOG = LoggerFactory.getLogger(PostAggregateSorting.class);
	
	private static SortedSet<Map.Entry<String, AggregateAPIEntity>> sortByValue(Map<String, AggregateAPIEntity> map, List<AggregateParams.SortFieldOrder> sortedFields) {
	    SortedSet<Map.Entry<String, AggregateAPIEntity>> sortedEntries = new TreeSet<Map.Entry<String, AggregateAPIEntity>>(new MapKeyValueComparator(sortedFields));
	    sortedEntries.addAll(map.entrySet());
	    return sortedEntries;
	}

	/**
	 * recursively populate sorted list from entity list
	 * @param entity
	 */
	public static void sort(AggregateAPIEntity entity, List<AggregateParams.SortFieldOrder> sortFieldOrders){
		// sort should internally add key field to AggregateAPIEntity before the sorting starts as "key" could be sorted against
		Map<String, AggregateAPIEntity> children = entity.getEntityList();
		for(Map.Entry<String, AggregateAPIEntity> e : children.entrySet()){
			e.getValue().setKey(e.getKey());
		}
		SortedSet<Map.Entry<String, AggregateAPIEntity>> set = sortByValue(children, sortFieldOrders);
		for(Map.Entry<String, AggregateAPIEntity> entry : set){
			entity.getSortedList().add(entry.getValue());
		}
		for(Map.Entry<String, AggregateAPIEntity> entry : entity.getEntityList().entrySet()){
			sort(entry.getValue(), sortFieldOrders);
		}
		entity.setEntityList(null);
	}

	private static class MapKeyValueComparator implements Comparator<Map.Entry<String, AggregateAPIEntity>>{
		private List<AggregateParams.SortFieldOrder> sortedFieldOrders;
		public MapKeyValueComparator(List<AggregateParams.SortFieldOrder> sortedFields){
			this.sortedFieldOrders = sortedFields;
		}
		@Override
        public int compare(Map.Entry<String, AggregateAPIEntity> e1, Map.Entry<String, AggregateAPIEntity> e2){
			int r = 0;
			AggregateAPIEntity entity1 = e1.getValue();
			AggregateAPIEntity entity2 = e2.getValue();
            for(AggregateParams.SortFieldOrder sortFieldOrder : sortedFieldOrders){
            	// TODO count should not be literal, compare numTotalDescendants
            	if(sortFieldOrder.getField().equals(AggregateParams.SortFieldOrder.SORT_BY_COUNT)){
            		long tmp = entity1.getNumTotalDescendants() - entity2.getNumTotalDescendants();
            		r = (tmp == 0) ? 0 : ((tmp > 0) ? 1 : -1);
            	}else if(sortFieldOrder.getField().equals(AggregateParams.SortFieldOrder.SORT_BY_AGGREGATE_KEY)){
            		r = entity1.getKey().compareTo(entity2.getKey());
            	}else{
            		try{
	            		String sortedField = sortFieldOrder.getField();
	            		String tmp1 = sortedField.substring(0, 1).toUpperCase()+sortedField.substring(1);
	            		Method getMethod1 = entity1.getClass().getMethod("get"+tmp1);
	            		Object r1 = getMethod1.invoke(entity1);
	            		Long comp1 = (Long)r1;
	            		String tmp2 = sortedField.substring(0, 1).toUpperCase()+sortedField.substring(1);
	            		Method getMethod2 = entity2.getClass().getMethod("get"+tmp2);
	            		Object r2 = getMethod2.invoke(entity2);
	            		Long comp2 = (Long)r2;
	            		r = comp1.compareTo(comp2);
            		}catch(Exception ex){
            			LOG.error("Can not get corresponding field for sorting", ex);
            			r = 0;
            		}
            	}
            	if(r == 0) continue;
        		if(!sortFieldOrder.isAscendant()){
        			r = -r;
        		}
    			return r;
            }	
			return r;
        }
	}
}
