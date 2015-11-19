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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public enum AggregateFunctionType{
	count("^(count)$"),
	sum("^sum\\((.*)\\)$"),
	avg("^avg\\((.*)\\)$"),
	max("^max\\((.*)\\)$"),
	min("^min\\((.*)\\)$");
	
	private Pattern pattern;
	private AggregateFunctionType(String patternString){
		this.pattern = Pattern.compile(patternString);
	}

	/**
	 * This method is thread safe
	 * match and retrieve back the aggregated fields, for count, aggregateFields can be null
	 * @param function
	 * @return
	 */
	public AggregateFunctionTypeMatcher matcher(String function){
		Matcher m = pattern.matcher(function);

		if(m.find()){
			return new AggregateFunctionTypeMatcher(this, true, m.group(1));
		}else{
			return new AggregateFunctionTypeMatcher(this, false, null);
		}
	}

	public static AggregateFunctionTypeMatcher matchAll(String function){
		for(AggregateFunctionType type : values()){
			Matcher m = type.pattern.matcher(function);
			if(m.find()){
				return new AggregateFunctionTypeMatcher(type, true, m.group(1));
			}
		}
		return new AggregateFunctionTypeMatcher(null, false, null);
	}

	public static byte[] serialize(AggregateFunctionType type){
		return type.name().getBytes();
	}

	public static AggregateFunctionType deserialize(byte[] type){
		return valueOf(new String(type));
	}

	public static List<byte[]> toBytesList(List<AggregateFunctionType> types){
		List<byte[]> result = new ArrayList<byte[]>();
		for(AggregateFunctionType type:types){
			result.add(serialize(type));
		}
		return result;
	}

	public static List<AggregateFunctionType> fromBytesList(List<byte[]> types){
		List<AggregateFunctionType> result = new ArrayList<AggregateFunctionType>();
		for(byte[] bs:types){
			result.add(deserialize(bs));
		}
		return result;
	}
}