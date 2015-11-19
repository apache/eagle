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

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

public class PostHierarchicalAggregateSort {

	private static SortedSet<Map.Entry<String, HierarchicalAggregateEntity>> sortByValue(HierarchicalAggregateEntity entity, List<SortOption> sortOptions) {
	    SortedSet<Map.Entry<String, HierarchicalAggregateEntity>> sortedEntries = new TreeSet<Map.Entry<String, HierarchicalAggregateEntity>>(new MapEntryComparator(sortOptions));
	    sortedEntries.addAll(entity.getChildren().entrySet());
	    return sortedEntries;
	}

	/**
	 * sort aggregated results with sort options
     *
     * @param result
     * @param sortOptions
     * @return
     */
	public static HierarchicalAggregateEntity sort(HierarchicalAggregateEntity result, List<SortOption> sortOptions){
		SortedSet<Map.Entry<String, HierarchicalAggregateEntity>> tmp = sortByValue(result, sortOptions);
		result.setSortedList(tmp);
		result.setChildren(null);
		for(Map.Entry<String, HierarchicalAggregateEntity> entry : tmp){
			sort(entry.getValue(), sortOptions);
		}
		return result;
	}

	private static class MapEntryComparator implements Comparator<Map.Entry<String, HierarchicalAggregateEntity>>{
		private List<SortOption> sortOptions;

		public MapEntryComparator(List<SortOption> sortOptions){
			this.sortOptions = sortOptions;
		}

		/**
		 * default to sort by all groupby fields
		 */
		@Override
        public int compare(Map.Entry<String, HierarchicalAggregateEntity> e1, Map.Entry<String, HierarchicalAggregateEntity> e2){
			int r = 0;
			String key1 = e1.getKey();
			List<Double> valueList1 = e1.getValue().getValues();
			String key2 = e2.getKey();
			List<Double> valueList2 = e2.getValue().getValues();
			for(SortOption so : sortOptions){
				int index = so.getIndex();
				if (index == -1) {
					continue;
				}
				if(!so.isInGroupby()){  // sort fields come from functions
					Double value1 = valueList1.get(index);
					Double value2 = valueList2.get(index);
					r = value1.compareTo(value2);
				}  
				// sort fields come from groupby fields, then silently ignored
				
				if(r == 0) continue;
				if(!so.isAscendant()){
					r = -r;
				}
				return r;
			}
			// default to sort by groupby fields ascendently
			if(r ==0){
				return key1.compareTo(key2);
			}
			return r;
        }
	}
}
