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

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public enum SortFieldOrderType {
	key("^(key)=(asc|desc)$"),
	count("^(count)=(asc|desc)$"),
	sum("^sum\\((.*)\\)=(asc|desc)$"),
	avg("^avg\\((.*)\\)(asc|desc)$"),
	max("^max\\((.*)\\)(asc|desc)$"),
	min("^min\\((.*)\\)(asc|desc)$");
	
	private Pattern pattern;
	private SortFieldOrderType(String patternString){
		this.pattern = Pattern.compile(patternString);
	}

	/**
	 * This method is thread safe
	 * match and retrieve back the aggregated fields, for count, aggregateFields can be null
	 * @param sortFieldOrder
	 * @return
	 */
	public SortFieldOrderTypeMatcher matcher(String sortFieldOrder){
		Matcher m = pattern.matcher(sortFieldOrder);
		
		if(m.find()){
			return new SortFieldOrderTypeMatcher(true, m.group(1), m.group(2));
		}else{
			return new SortFieldOrderTypeMatcher(false, null, null);
		}
	}
	
	public static AggregateParams.SortFieldOrder matchAll(String sortFieldOrder){
		for(SortFieldOrderType type : SortFieldOrderType.values()){
			SortFieldOrderTypeMatcher m = type.matcher(sortFieldOrder);
			if(m.find())
				return m.sortFieldOrder();
		}
		return null;
	}
}
