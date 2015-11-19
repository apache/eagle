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
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

public class PostFlatAggregateSort {
	private static SortedSet<Map.Entry<List<String>, List<Double>>> sortByValue(Map<List<String>, List<Double>> map, List<SortOption> sortOptions) {
	    SortedSet<Map.Entry<List<String>, List<Double>>> sortedEntries = new TreeSet<Map.Entry<List<String>, List<Double>>>(new MapEntryComparator(sortOptions));
	    sortedEntries.addAll(map.entrySet());
	    return sortedEntries;
	}

	/**
	 * sort aggregated results with sort options
	 * @param aggregatedResult aggregated result set, but it is not sorted
	 * @sortOptions sorting options
	 * @topN top N results will be returned if topN is specified. If it's not specified (as default value 0), all results will be returned
	 */
	public static List<Map.Entry<List<String>, List<Double>>> sort(Map<List<String>, List<Double>> aggregatedResult, List<SortOption> sortOptions, int topN){
		SortedSet<Map.Entry<List<String>, List<Double>>> allList = sortByValue(aggregatedResult, sortOptions);
		List<Map.Entry<List<String>, List<Double>>> result = new ArrayList<Map.Entry<List<String>, List<Double>>>();
		for (Map.Entry<List<String>, List<Double>> entry : allList) {
			result.add(entry);
			if (topN > 0 && result.size() >= topN) {
				break;
			}
		}
		return result;
	}

	private static class MapEntryComparator implements Comparator<Map.Entry<List<String>, List<Double>>>{
		private List<SortOption> sortOptions;
		public MapEntryComparator(List<SortOption> sortOptions){
			this.sortOptions = sortOptions;
		}
		/**
		 * default to sort by all groupby fields
		 */
		@Override
        public int compare(Map.Entry<List<String>, List<Double>> e1, Map.Entry<List<String>, List<Double>> e2){
			int r = 0;
			List<String> keyList1 = e1.getKey();
			List<Double> valueList1 = e1.getValue();
			List<String> keyList2 = e2.getKey();
			List<Double> valueList2 = e2.getValue();
			for(SortOption so : sortOptions){
				int index = so.getIndex();
				if (index == -1) {
					continue;
				}
				if(!so.isInGroupby()){  // sort fields come from functions
					Double value1 = valueList1.get(index);
					Double value2 = valueList2.get(index);
					r = value1.compareTo(value2);
				}else{  // sort fields come from groupby fields
					String key1 = keyList1.get(index);
					String key2 = keyList2.get(index);
					r = key1.compareTo(key2);
				}
				if(r == 0) continue;
				if(!so.isAscendant()){
					r = -r;
				}
				return r;
			}
			// default to sort by groupby fields ascendently
			if(r ==0){ // TODO is this check necessary
				return new GroupbyFieldsComparator().compare(keyList1, keyList2);
			}
			return r;
        }
	}
}
