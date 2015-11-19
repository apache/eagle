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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SortOptionsParser {
	private static final Logger LOG = LoggerFactory.getLogger(SortOptionsParser.class);
	private static Pattern pattern = Pattern.compile("^(.+)\\s+(asc|desc)$");
		
	public static List<SortOption> parse(List<String> groupbyFields, List<String> aggregatedFields, List<String> sortOptions, List<String> sortFields){
		List<SortOption> list = new ArrayList<SortOption>();
		for(String sortOption : sortOptions){
			Matcher m = pattern.matcher(sortOption);
			if(!m.find()){
				throw new IllegalArgumentException("sort option must have the format of <groupbyfield|function> asc|desc");
			}
			String field = m.group(1);
			if (sortFields != null) {
				sortFields.add(field);
			}
			SortOption so = new SortOption();
			list.add(so);
			so.setAscendant(m.group(2).equals("asc") ? true : false);
			int index = aggregatedFields.indexOf(field); 
			if(index > -1){
				so.setInGroupby(false);
				so.setIndex(index);
				continue;
			}
			if(groupbyFields != null){  // if groupbyFields is not provided, ignore this sort field
				index = groupbyFields.indexOf(field);
				if(index > -1){
					so.setInGroupby(true);
					so.setIndex(index);
					continue;
				}
			}
			logNonExistingSortByField(field);
			so.setInGroupby(false);
			so.setIndex(-1);
		}
		return list;
	}
	
	private static void logNonExistingSortByField(String sortByField){
		LOG.warn("Sortby field is neither in aggregated fields or groupby fields, ignore " + sortByField);
	}
}
