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
package org.apache.eagle.query.aggregate.raw;

import org.apache.eagle.log.entity.EntityQualifierUtils;
import org.apache.eagle.log.entity.GenericMetricEntity;
import org.apache.eagle.log.entity.meta.*;
import org.apache.eagle.log.expression.ExpressionParser;
import org.apache.eagle.query.aggregate.AggregateFunctionType;
import org.apache.eagle.query.parser.TokenConstant;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class RawGroupbyBucket {
	private final static Logger LOG = LoggerFactory.getLogger(RawGroupbyBucket.class);

	private List<String> aggregatedFields;
	private EntityDefinition entityDefinition;

	
	private List<AggregateFunctionType> types;
	private SortedMap<GroupbyKey, List<Function>> group2FunctionMap =
			new TreeMap<GroupbyKey, List<Function>>(new GroupbyKeyComparator());

	public RawGroupbyBucket(List<AggregateFunctionType> types, List<String> aggregatedFields, EntityDefinition ed){
		this.types = types;
		this.aggregatedFields = aggregatedFields;
		this.entityDefinition = ed;
	}

	public boolean exists(GroupbyKey key){
		return group2FunctionMap.containsKey(key);
	}

	public void addDatapoint(GroupbyKey groupbyKey, Map<String, byte[]> values){
		// locate groupby bucket
		List<Function> functions = group2FunctionMap.get(groupbyKey);
		if(functions == null){
			functions = new ArrayList<Function>();
			for(AggregateFunctionType type : types){
				FunctionFactory ff = FunctionFactory.locateFunctionFactory(type);
				if(ff == null){
					LOG.error("FunctionFactory of AggregationFunctionType:"+type+" is null");
				}else{
					functions.add(ff.createFunction());
				}
			}
			group2FunctionMap.put(groupbyKey, functions);
		}
		ListIterator<Function> e1 = functions.listIterator();
		ListIterator<String> e2 = aggregatedFields.listIterator();
		while(e1.hasNext() && e2.hasNext()){
			Function f = e1.next();
			String aggregatedField = e2.next();
			byte[] v = values.get(aggregatedField);
			if(f instanceof Function.Count){ // handle count
				if(entityDefinition.getMetricDefinition()==null) {
					f.run(1.0);
					continue;
				}else if(v == null){
					aggregatedField = GenericMetricEntity.VALUE_FIELD;
					v = values.get(aggregatedField);
				}
			}
			if(v != null){
				Qualifier q = entityDefinition.getDisplayNameMap().get(aggregatedField);
				EntitySerDeser<?> serDeser = q.getSerDeser();
				// double d = 0.0;
				if(serDeser instanceof IntSerDeser){
					double d= (Integer)serDeser.deserialize(v);
					f.run(d);
				}else if(serDeser instanceof LongSerDeser){
					double d = (Long)serDeser.deserialize(v);
					f.run(d);
				}else if(serDeser instanceof DoubleSerDeser){
					double d = (Double)serDeser.deserialize(v);
					f.run(d);
				// TODO: support numeric array type that is not metric
				}else if(serDeser instanceof DoubleArraySerDeser){
					double[] d = ((DoubleArraySerDeser) serDeser).deserialize(v);
					if(f instanceof Function.Count){
						f.run(d.length);
					} else {
						for(double i:d) f.run(i);
					}
				}else if(serDeser instanceof IntArraySerDeser){
					int[] d = ((IntArraySerDeser) serDeser).deserialize(v);
					if(f instanceof Function.Count){
						f.run(d.length);
					}else{
						for(int i:d) f.run(i);
					}
				}else{
					if(LOG.isDebugEnabled()) LOG.debug("EntitySerDeser of field "+aggregatedField+" is not IntSerDeser or LongSerDeser or DoubleSerDeser or IntArraySerDeser or DoubleArraySerDeser, default as 0.0");
				}
			}else if(TokenConstant.isExpression(aggregatedField)){
				String expression = TokenConstant.parseExpressionContent(aggregatedField);
				try {
					Map<String,Double> doubleMap = EntityQualifierUtils.bytesMapToDoubleMap(values, entityDefinition);
					if(entityDefinition.getMetricDefinition() == null) {
						double value = ExpressionParser.eval(expression,doubleMap);
						// LOG.info("DEBUG: Eval "+expression +" = "+value);
						f.run(value);
					}else{
						Qualifier qualifier = entityDefinition.getDisplayNameMap().get(GenericMetricEntity.VALUE_FIELD);
						EntitySerDeser _serDeser = qualifier.getSerDeser();
						byte[] valueBytes = values.get(GenericMetricEntity.VALUE_FIELD);
						if( _serDeser instanceof DoubleArraySerDeser){
							double[] d = (double[]) _serDeser.deserialize(valueBytes);
							if(f instanceof Function.Count) {
								f.run(d.length);
							}else{
								for(double i:d){
									doubleMap.put(GenericMetricEntity.VALUE_FIELD,i);
									f.run(ExpressionParser.eval(expression, doubleMap));
								}
							}
						}else if(_serDeser instanceof IntArraySerDeser){
							int[] d = (int[]) _serDeser.deserialize(valueBytes);
							if(f instanceof Function.Count) {
								f.run(d.length);
							}else {
								for (double i : d) {
									doubleMap.put(GenericMetricEntity.VALUE_FIELD, i);
									f.run(ExpressionParser.eval(expression, doubleMap));
								}
							}
						}else{
							double value = ExpressionParser.eval(expression,doubleMap);
							f.run(value);
						}
					}
				} catch (Exception e) {
					LOG.error("Got exception to evaluate expression: "+expression+", exception: "+e.getMessage(),e);
				}
			}
		}
	}

	/**
	 * expensive operation - create objects and format the result
	 * @return
	 */
	public List<GroupbyKeyValue> groupbyKeyValues(){
		List<GroupbyKeyValue> results = new ArrayList<GroupbyKeyValue>();
		for(Map.Entry<GroupbyKey, List<Function>> entry : this.group2FunctionMap.entrySet()){
			GroupbyValue value = new GroupbyValue();
			for(Function f : entry.getValue()){
				value.add(new DoubleWritable(f.result()));
				value.addMeta(f.count());
			}
			results.add(new GroupbyKeyValue(entry.getKey(),value));
		}
		return results;
	}

	/**
	 * expensive operation - create objects and format the result
	 * @return
	 */
	public Map<List<String>, List<Double>> result(){
		Map<List<String>, List<Double>> result = new HashMap<List<String>, List<Double>>();
		for(Map.Entry<GroupbyKey, List<Function>> entry : this.group2FunctionMap.entrySet()){
			List<Double> values = new ArrayList<Double>();
			for(Function f : entry.getValue()){
				values.add(f.result());
			}
			GroupbyKey key = entry.getKey();
			List<BytesWritable> list1 = key.getValue();
			List<String> list2 = new ArrayList<String>();
			for(BytesWritable e : list1){
				list2.add(new String(e.copyBytes()));
			}
			result.put(list2, values);
		}
		return result;
	}
}
