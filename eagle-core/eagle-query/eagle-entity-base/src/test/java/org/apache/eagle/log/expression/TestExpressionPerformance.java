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
/**
 * 
 */
package org.apache.eagle.log.expression;

import org.junit.Assert;
import org.junit.Test;
import parsii.eval.Expression;
import parsii.eval.Parser;
import parsii.eval.Scope;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/**
 * @since Nov 4, 2014
 */

public class TestExpressionPerformance {

	public interface ExpressionParser {
		double parse(String exprStr, Map<String, Double> tuple) throws Exception;
	}
	
	public class ParsiiParser implements ExpressionParser{
		public Expression expression;
		
		public double parse(String exprStr, Map<String, Double> tuple) throws Exception{		
			Scope scope = Scope.create();
			if (expression == null) {
				expression = Parser.parse(exprStr, scope);
			}
			for(String valName : tuple.keySet()) {
				Object value = tuple.get(valName);
				if(value instanceof Number) {
					scope.getVariable(valName).setValue(((Number)value).doubleValue());
				}
			}
			return expression.evaluate();
		}
	}
	
	public long doParse(ExpressionParser parser, String exprStr, List<String> parameters) throws Exception{
		long startTime = System.currentTimeMillis();
		int parNum = parameters.size();
		Map<String, Double> tuple = new HashMap<String, Double>();
		for (int i = 1; i < 100000; i++) {
			for (int j = 0; j < parNum; j++) {
				tuple.put(parameters.get(j), (double) (i * 3 + j));				
			}
			parser.parse(exprStr, tuple);
		}
		long endTime = System.currentTimeMillis();
		return endTime - startTime;
	}
	
	@Test
	public void TestPerformance() throws Exception{
		List<ExpressionParser> parsers = new ArrayList<ExpressionParser>();
		parsers.add(new ParsiiParser());

		String exprStr = "a + b / c * 2"; 
		List<String> parameters = new ArrayList<String>();
		parameters.add("a");
		parameters.add("b");
		parameters.add("c");
		
		Map<String, Long> timeComsued = new HashMap<String, Long>();
		
		for (int i = 0; i < 10; i++) {
			for (ExpressionParser parser : parsers) {
				String name = parser.getClass().getName();
				if (timeComsued.get(name) == null) {
					timeComsued.put(name, 0L);
				}
				timeComsued.put(name, timeComsued.get(name) + doParse(parser, exprStr, parameters));			
			}
		}
		for (Entry<String, Long> time : timeComsued.entrySet()) {
			System.out.println("time consumed of " + time.getKey() + ": " + time.getValue() +"ms");
		}
	}

	@Test
	public void TestEvaluatoinValid() throws Exception{
		List<ExpressionParser> parsers = new ArrayList<ExpressionParser>();
		parsers.add(new ParsiiParser());

		String exprStr = "max(a, 3 * b) + min(b, 10000) / abs(c * 2)";
		Map<String ,Double> tuples = new HashMap<String, Double>();
		tuples.put("a", 20.5);
		tuples.put("b", 123.7);
		tuples.put("c", 97.57);
		DecimalFormat df = new DecimalFormat("#.00");
		for (ExpressionParser parser : parsers) {			
			System.out.println(parser.getClass().getName() + " : " + parser.parse(exprStr, tuples));
			Assert.assertEquals(df.format(parser.parse(exprStr, tuples)), "371.73");
		}
	}
}
