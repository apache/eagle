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

/**
 * this is default comparator for aggregation. The behavior is to sort by groupby fields ascendantly
 */
public class GroupbyFieldsComparator implements Comparator<List<String>>{
	@Override 
    public int compare(List<String> list1, List<String> list2){
		if(list1 == null || list2 == null || list1.size() != list2.size())
			throw new IllegalArgumentException("2 list of groupby fields must be non-null and have the same size");
		int r = 0;
		int index = 0;
		for(String s1 : list1){
			r = s1.compareTo(list2.get(index++));
			if(r != 0)
				return r;
		}
		return r;
	}
}
