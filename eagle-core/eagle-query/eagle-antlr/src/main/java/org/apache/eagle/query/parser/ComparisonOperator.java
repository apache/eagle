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
package org.apache.eagle.query.parser;

public enum ComparisonOperator {
	EQUAL("="),
	LIKE("=~"),
	IN("IN"),
	NOT_IN("NOT IN"),
	LESS("<"),
	LESS_OR_EQUAL("<="),
	GREATER(">"),
	GREATER_OR_EQUAL(">="),
	NOT_EQUAL("!="),
	NOT_LIKE("!=~"),
	CONTAINS("CONTAINS"),
	NOT_CONTAINS("NOT CONTAINS"),
	IS("IS"),
	IS_NOT("IS NOT");

	private final String _op;
	private ComparisonOperator(String op){
		_op = op;
	}
	
	public String toString(){
		return _op;
	}
	
	public static ComparisonOperator locateOperator(String op){
		op = op.replaceAll("\\s+"," ");
		for(ComparisonOperator o : ComparisonOperator.values()){
			if(op.toUpperCase().equals(o._op)){
				return o;
			}
		}
		throw new UnsupportedExpressionOperatorException(op);
	}
}


