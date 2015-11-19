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
package org.apache.eagle.query.parser.test;

import org.apache.eagle.query.parser.TokenConstant;
import org.apache.eagle.query.parser.TokenType;
import junit.framework.Assert;
import org.junit.Test;

public class TestValueType {
	@Test
	public void testLocateValueType(){
		Assert.assertEquals(TokenType.EXP, TokenType.locate("EXP{ 1+1 = 2 }"));
		Assert.assertEquals(TokenType.EXP, TokenType.locate("EXP{ sum(a + b) > 1 }"));

		Assert.assertEquals(TokenType.STRING, TokenType.locate("\"\""));
		Assert.assertEquals(TokenType.STRING, TokenType.locate("\"abc\""));

		Assert.assertEquals(TokenType.LIST, TokenType.locate("(1,\"ab\")"));
		Assert.assertEquals(TokenType.LIST, TokenType.locate("(\"\",\"ab\")"));

		Assert.assertEquals(TokenType.NUMBER, TokenType.locate("1"));
		Assert.assertEquals(TokenType.NUMBER, TokenType.locate("1.234"));
		Assert.assertEquals(TokenType.NUMBER, TokenType.locate("-1.234"));
		Assert.assertEquals(TokenType.NUMBER, TokenType.locate("+1.234"));
		Assert.assertEquals(TokenType.NUMBER, TokenType.locate("- 1.234"));
		Assert.assertEquals(TokenType.NUMBER, TokenType.locate("+ 1.234"));
		Assert.assertEquals(TokenType.NUMBER, TokenType.locate("    + 1.234 "));
		Assert.assertEquals(TokenType.NUMBER, TokenType.locate("    + 1.234     "));

		Assert.assertEquals(TokenType.NULL, TokenType.locate("null"));
		Assert.assertEquals(TokenType.NULL, TokenType.locate("NULL"));

		Assert.assertEquals(TokenType.STRING,TokenType.locate("\"SELECT start.hr AS hr,\n" +
				" ...details.inst_type(Stage-10)\""));

		// Bad format
		boolean gotEx = false;
		try{
			TokenType.locate("+ 1.234.567");
		}catch (IllegalArgumentException ex){
			gotEx = true;
		}
		Assert.assertTrue(gotEx);
	}

	@Test
	public void testParseExpressionContent(){
		String expression = "EXP{ @fieldName /2 } AS a";
		Assert.assertEquals(" @fieldName /2 ", TokenConstant.parseExpressionContent(expression));


		expression = "EXP{ @fieldName /2 } a";
		Assert.assertEquals(" @fieldName /2 ", TokenConstant.parseExpressionContent(expression));

		expression = "EXP{ @fieldName /2 }";
		Assert.assertEquals(" @fieldName /2 ", TokenConstant.parseExpressionContent(expression));
	}
}