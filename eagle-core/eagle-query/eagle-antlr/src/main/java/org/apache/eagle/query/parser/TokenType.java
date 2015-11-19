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

public enum TokenType {
	EXP, NUMBER, STRING, LIST, NULL,ID;

	public static TokenType locate(String tokenText){
		String _value = tokenText.trim();
		if (TokenConstant.EXP_PATTERN.matcher(_value).matches()) {
			// e.q. "value" with EXP{...}
			return EXP;
		}else if(TokenConstant.STRING_PATTERN.matcher(_value).matches()){
			// e.q. "value" with quotes
			return STRING;
		}else if(TokenConstant.ARRAY_PATTERN.matcher(_value).matches()){
			// e.q. (item1,item2,..)
			return LIST;
		}else if(TokenConstant.NUMBER_PATTERN.matcher(_value).matches()){
			// e.q. 1.32 without quotes
			return NUMBER;
		}else if(TokenConstant.NULL_PATTERN.matcher(_value).matches()){
			// e.q. null or NULL without quotes
			return NULL;
		} else if(TokenConstant.ID_PATTERN.matcher(_value).matches()){
			return ID;
		}
		throw new IllegalArgumentException("Can't locate type of value text: "+tokenText);
	}
}