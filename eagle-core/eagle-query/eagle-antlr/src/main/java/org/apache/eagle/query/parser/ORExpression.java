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

import java.util.ArrayList;
import java.util.List;

public class ORExpression {
	private List<ANDExpression> andExprList = new ArrayList<ANDExpression>();

	public List<ANDExpression> getANDExprList() {
		return andExprList;
	}

	public void setANDExprList(List<ANDExpression> list) {
		this.andExprList = list;
	}
	
	public String toString(){
		StringBuffer sb = new StringBuffer();
		boolean first = true;
		for(ANDExpression andRel : andExprList){
			if(first)
				first = false;
			else{
				sb.append(" ");
				sb.append(LogicalOperator.OR);
				sb.append(" ");
			}
			sb.append("(");
			boolean firstAND = true;
			for(AtomicExpression kv : andRel.getAtomicExprList()){
				if(firstAND)
					firstAND = false;
				else{
					sb.append(" ");
					sb.append(LogicalOperator.AND);
					sb.append(" ");
				}
				sb.append(kv);
			}
			sb.append(")");
		}
		return sb.toString();
	}
}
