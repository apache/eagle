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
package org.apache.eagle.log.expression;
/**
 * 
 */

import junit.framework.Assert;
import org.junit.Test;

import java.util.List;

/**
 * @since Nov 10, 2014
 */
public class TestExpressionParser {
			
	@Test
	public void testSingleVariable() throws Exception{
		String exprStr = "mapProgress";		
		ExpressionParser parser = new ExpressionParser(exprStr);
		Double value = parser.setVariable("mapProgress", 100.0)
							 .eval();
		Assert.assertEquals(value, 100.0);
		List<String> dependentFields = parser.getDependentFields();
		Assert.assertEquals(dependentFields.size(), 1);
		Assert.assertEquals(dependentFields.get(0), "mapProgress");
	}
	
	@Test
	public void testgetDependency() throws Exception{
		/** NOTICE: expression should be enclosure with "EXP{}" , This is for making antlr easy to parse  
		  * variable name cannot be "pi" OR "E", there are parssi builtin constants */
		String exprStr = "min(mAx, Max) / abs(MAX)";
		ExpressionParser parser = new ExpressionParser(exprStr);
		List<String> variables =  parser.getDependentFields();
		Assert.assertEquals(variables.size(), 3);
		Assert.assertTrue(variables.contains("mAx"));
		Assert.assertTrue(variables.contains("Max"));
		Assert.assertTrue(variables.contains("MAX"));
	}

	@Test
	public void testFunction() throws Exception{
		String exprStr = "min(mapProgress, reduceProgress) / abs(endTime - startTime)";
		ExpressionParser parser = new ExpressionParser(exprStr);
		Double value = parser.setVariable("mapProgress", 100.0)
							 .setVariable("reduceProgress", 20.0)
							 .setVariable("endTime", 1415590100000.0)
							 .setVariable("startTime", 1415590000000.0)
							 .eval();
		Assert.assertEquals(value, 0.0002);
	}
	
	@Test
	public void testOperator() throws Exception{
		String exprStr = "(a+b*c) / (2*(d-e))";
		ExpressionParser parser = new ExpressionParser(exprStr);
		Double value = parser.setVariable("a", 200.0)
							 .setVariable("b", 400.0)
							 .setVariable("c", 3.0)
							 .setVariable("d", 225.0)
							 .setVariable("e", -125.0)
							 .eval();
		Assert.assertEquals(value, 2.0);
	}
	
	@Test
	public void testOperatorWithFunction() throws Exception{
		String exprStr = "(max(a, b)* min(a, b)) / abs(a-b+c-d)";
		ExpressionParser parser = new ExpressionParser(exprStr);
		Double value = parser.setVariable("a", 300.0)
							 .setVariable("b", 200.0)
							 .setVariable("c", -300.0)
							 .setVariable("d", -300.0)
							 .eval();
		Assert.assertEquals(value, 600.0);
	}

	@Test
	public void testWithAtFieldName() throws Exception{
		String exprStr = "(max(a, b)* min(a, b)) / abs(a-b+c-d)";
		ExpressionParser parser = new ExpressionParser(exprStr);
		Double value = parser.setVariable("a", 300.0)
							 .setVariable("b", 200.0)
							 .setVariable("c", -300.0)
							 .setVariable("d", -300.0)
							 .eval();
		Assert.assertEquals(value, 600.0);
	}

	@Test
	public void testConstant() throws Exception {
		String exprStr = "a";
		ExpressionParser parser = new ExpressionParser(exprStr);
		Double value = parser.setVariable("a", 300.0)
				.setVariable("b", 200.0)
				.setVariable("c", -300.0)
				.setVariable("d", -300.0)
				.eval();
		Assert.assertEquals(value, 300.0);

		value = parser.setVariable("a", 200.0)
				.setVariable("b", 200.0)
				.setVariable("c", -300.0)
				.setVariable("d", -300.0)
				.eval();
		Assert.assertEquals(value, 200.0);
	}

	@Test
	public void testBooleanExpression() throws Exception {
		String exprStr = "a > b";
		ExpressionParser parser = new ExpressionParser(exprStr);
		Double value = parser.setVariable("a", 300.0)
				.setVariable("b", 200.0)
				.setVariable("c", -300.0)
				.setVariable("d", -300.0)
				.eval();
		Assert.assertEquals(value, 1.0);

		value = parser.setVariable("a", 100.0)
				.setVariable("b", 200.0)
				.setVariable("c", -300.0)
				.setVariable("d", -300.0)
				.eval();
		Assert.assertEquals(value, 0.0);

		exprStr = "a < b";
		parser = new ExpressionParser(exprStr);
		value = parser.setVariable("a", 300.0)
				.setVariable("b", 300.0)
				.setVariable("c", -300.0)
				.setVariable("d", -300.0)
				.eval();
		Assert.assertTrue(value == 0.0);

		value = parser.setVariable("a", 400.0)
				.setVariable("b", 300.0)
				.setVariable("c", -300.0)
				.setVariable("d", -300.0)
				.eval();
		Assert.assertTrue(value == 0.0);

		value = parser.setVariable("a", 100.0)
				.setVariable("b", 200.0)
				.setVariable("c", -300.0)
				.setVariable("d", -300.0)
				.eval();
		Assert.assertTrue(value == 1.0);

		// !!! Not support well >=
		exprStr = "a >= b";
		parser = new ExpressionParser(exprStr);
		value = parser.setVariable("a", 300.0)
				.setVariable("b", 300.0)
				.setVariable("c", -300.0)
				.setVariable("d", -300.0)
				.eval();
		Assert.assertTrue(value == 0.0); // expect 1.0

		value = parser.setVariable("a", 400.0)
				.setVariable("b", 300.0)
				.setVariable("c", -300.0)
				.setVariable("d", -300.0)
				.eval();
		Assert.assertTrue(value == 1.0); // expect 1.0

		value = parser.setVariable("a", 100.0)
				.setVariable("b", 200.0)
				.setVariable("c", -300.0)
				.setVariable("d", -300.0)
				.eval();
		Assert.assertTrue(value == 1.0); // expect 0.0

		exprStr = "a <= b";
		parser = new ExpressionParser(exprStr);
		value = parser.setVariable("a", 300.0)
				.setVariable("b", 300.0)
				.setVariable("c", -300.0)
				.setVariable("d", -300.0)
				.eval();
		Assert.assertTrue(value == 1.0);

		value = parser.setVariable("a", 400.0)
				.setVariable("b", 300.0)
				.setVariable("c", -300.0)
				.setVariable("d", -300.0)
				.eval();
		Assert.assertTrue(value == 0.0);

		value = parser.setVariable("a", 100.0)
				.setVariable("b", 200.0)
				.setVariable("c", -300.0)
				.setVariable("d", -300.0)
				.eval();
		Assert.assertTrue(value == 1.0);

		exprStr = "a = b";
		parser = new ExpressionParser(exprStr);
		value = parser.setVariable("a", 300.0)
				.setVariable("b", 300.0)
				.setVariable("c", -300.0)
				.setVariable("d", -300.0)
				.eval();
		Assert.assertEquals(value, 1.0);

		value = parser.setVariable("a", 100.0)
				.setVariable("b", 200.0)
				.setVariable("c", -300.0)
				.setVariable("d", -300.0)
				.eval();
		Assert.assertEquals(value, 0.0);
	}

	@Test
	public void testParsiiBug() throws Exception {
		// !!! Not support >=
		String exprStr = "a >= b";
		ExpressionParser parser = new ExpressionParser(exprStr);
		Double value = parser.setVariable("a", 300.0)
				.setVariable("b", 300.0)
				.setVariable("c", -300.0)
				.setVariable("d", -300.0)
				.eval();
		Assert.assertTrue(value == 0.0); // expect 1.0

		value = parser.setVariable("a", 400.0)
				.setVariable("b", 300.0)
				.setVariable("c", -300.0)
				.setVariable("d", -300.0)
				.eval();
		Assert.assertTrue(value == 1.0); // expect 1.0

		value = parser.setVariable("a", 100.0)
				.setVariable("b", 200.0)
				.setVariable("c", -300.0)
				.setVariable("d", -300.0)
				.eval();
		Assert.assertTrue(value == 1.0); // expect 0.0
	}
}
