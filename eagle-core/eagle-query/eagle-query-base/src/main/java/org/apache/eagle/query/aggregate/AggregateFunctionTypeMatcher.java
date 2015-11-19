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

public class AggregateFunctionTypeMatcher {
	private final AggregateFunctionType type;
	private final boolean matched;
	private final String field;

	public AggregateFunctionTypeMatcher(AggregateFunctionType type, boolean matched, String field){
		this.type = type;
		this.matched = matched;
		this.field = field;
	}
	
	public boolean find(){
		return this.matched;
	}
	
	public String field(){
		return this.field;
	}
	
	public AggregateFunctionType type(){
		return this.type;
	}
}
