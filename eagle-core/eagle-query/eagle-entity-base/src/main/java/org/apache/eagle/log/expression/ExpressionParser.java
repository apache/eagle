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

import org.apache.eagle.log.base.taggedlog.TaggedLogAPIEntity;
import org.apache.eagle.log.entity.EntityQualifierUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import parsii.eval.Expression;
import parsii.eval.Parser;
import parsii.eval.Scope;
import parsii.eval.Variable;
import parsii.tokenizer.ParseException;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/**
 * <h1>Expression Evaluation</h1>
 *
 * Given expression in string and set context variables, return value in double
 *
 * <br/>
 * <br/>
 * For example:
 * <code>EXP{(max(a, b)* min(a, b)) / abs(a-b+c-d)} => 600.0</code>
 *
 * <br/>
 * <br/>
 * <b>NOTE:</b>  Expression variable <b>must</b> be in format: <code>fieldName</code> instead of <code>@fieldName</code>
 *
 * <br/>
 * <br/>
 * <h2>Dependencies:</h2>
 * <ul>
 *     <li>
 *         <a href="https://github.com/scireum/parsii">scireum/parsii</a>
 *         <i>Super fast and simple evaluator for mathematical expressions written in Java</i>
 *     </li>
 * </ul>
 *
 */
public class ExpressionParser{
	private final static Logger LOG = LoggerFactory.getLogger(ExpressionParser.class);

	private String exprStr;
	private Expression expression;
	private Scope scope;

	@SuppressWarnings("unused")
	public Scope getScope() {
		return scope;
	}

	private List<String> dependentFields;

	/**
	 * @param exprStr expression string in format like: <code>(max(a, b)* min(a, b)) / abs(a-b+c-d)</code>
	 *
	 * @throws ParseException
	 * @throws ParsiiInvalidException
	 */
	public ExpressionParser(String exprStr) throws ParseException, ParsiiInvalidException{
		this.exprStr = exprStr;
		scope = Scope.create();
		expression = Parser.parse(this.exprStr,scope);
	}

	@SuppressWarnings("unused")
	public ExpressionParser(String exprStr, Map<String, Double> context) throws ParsiiInvalidException, ParseException, ParsiiUnknowVariableException {
		this(exprStr);
		setVariables(context);
	}
	
	public ExpressionParser setVariables(Map<String, Double> tuple) throws ParsiiUnknowVariableException{
//		for(String valName : tuple.keySet()) {
//			Double value = tuple.get(valName);
		for(Map.Entry<String,Double> entry : tuple.entrySet()) {
            String valName = entry.getKey();
            Double value = entry.getValue();
			Variable variable = scope.getVariable(valName);
			if(variable!=null && value !=null) {
				variable.setValue(value);
			}else{
				if(LOG.isDebugEnabled()) LOG.warn("Variable for "+valName+" is null in scope of expression: "+this.exprStr);
			}
		}
		return this;
	}

	@SuppressWarnings("unused")
	public ExpressionParser setVariable(Entry<String, Double> tuple) throws ParsiiUnknowVariableException{
		if (getDependentFields().contains(tuple.getKey())) {
			scope.getVariable(tuple.getKey()).setValue(tuple.getValue());
		}
		else {
			throw new ParsiiUnknowVariableException("unknown variable: " + tuple.getKey());
		}
		return this;
	}
	
	public ExpressionParser setVariable(String key, Double value) throws ParsiiUnknowVariableException{
		scope.getVariable(key).setValue(value);
		return this;
	}

	public double eval() throws Exception{
		return expression.evaluate();
	}

	/**
	 * Thread safe
	 *
	 * @param tuple
	 * @return
	 * @throws ParsiiUnknowVariableException
	 */
	public double eval(Map<String, Double> tuple) throws Exception {
		synchronized (this){
			this.setVariables(tuple);
			return this.eval();
		}
	}

	public List<String> getDependentFields() {
		if (dependentFields == null) {
			dependentFields = new ArrayList<String>();
			for (String variable : scope.getNames()) {
				if (!variable.equals("pi") && !variable.equals("E") && !variable.equals("euler"))
					dependentFields.add(variable);
			}
		}
		return dependentFields; 
	}

	private final static Map<String, ExpressionParser> _exprParserCache = new HashMap<String, ExpressionParser>();

	/**
	 * Thread safe
	 *
	 * @param expr
	 * @return
	 * @throws ParsiiInvalidException
	 * @throws ParseException
	 */
	public static ExpressionParser parse(String expr) throws ParsiiInvalidException, ParseException {
		if(expr == null) throw new IllegalStateException("Expression to parse is null");
		synchronized (_exprParserCache) {
			ExpressionParser parser = _exprParserCache.get(expr);
			if (parser == null) {
				parser = new ExpressionParser(expr);
				_exprParserCache.put(expr, parser);
			}
			return parser;
		}
	}
	public static double eval(String expression,Map<String,Double> context) throws Exception {
		ExpressionParser parser = parse(expression);
		return parser.eval(context);
	}

	private static final Map<String,Method> _entityMethodCache = new HashMap<String, Method>();
	public static double eval(String expression,TaggedLogAPIEntity entity) throws Exception {
		ExpressionParser parser = parse(expression);
		List<String> dependencies = parser.getDependentFields();
		Map<String,Double> context = new HashMap<String,Double>();
		for(String field:dependencies){
			String methodName = "get"+field.substring(0, 1).toUpperCase() + field.substring(1);
			String methodUID = entity.getClass().getName()+"."+methodName;

			Method m;
			synchronized (_entityMethodCache) {
				m = _entityMethodCache.get(methodUID);
				if (m == null) {
					m = entity.getClass().getMethod(methodName);
					_entityMethodCache.put(methodUID, m);
				}
			}
			Object obj = m.invoke(entity);
			Double doubleValue = EntityQualifierUtils.convertObjToDouble(obj);
			// if(doubleValue == Double.NaN) throw new IllegalArgumentException("Field "+field+": "+obj+" in expression "+expression+" is not number");
			context.put(field,doubleValue);
		}
		return parser.eval(context);
	}
}
