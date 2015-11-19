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
package org.apache.eagle.query.aggregate.timeseries;

import org.apache.eagle.query.QueryConstants;
import org.apache.eagle.query.aggregate.AggregateFunctionType;
import org.apache.eagle.query.aggregate.raw.GroupbyKey;
import org.apache.eagle.query.aggregate.raw.GroupbyKeyValue;
import org.apache.eagle.query.aggregate.raw.GroupbyValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class GroupbyBucket {
	private final static Logger LOG = LoggerFactory.getLogger(GroupbyBucket.class);
	
	public static Map<String, FunctionFactory> _functionFactories = 
			new HashMap<>();
    
	// TODO put this logic to AggregatorFunctionType
	static{
		_functionFactories.put(AggregateFunctionType.count.name(), new CountFactory());
		_functionFactories.put(AggregateFunctionType.sum.name(), new SumFactory());
		_functionFactories.put(AggregateFunctionType.min.name(), new MinFactory());
		_functionFactories.put(AggregateFunctionType.max.name(), new MaxFactory());
		_functionFactories.put(AggregateFunctionType.avg.name(), new AvgFactory());
	}
	
	private List<AggregateFunctionType> types;
//	private SortedMap<List<String>, List<Function>> group2FunctionMap = 
//			new TreeMap<List<String>, List<Function>>(new GroupbyFieldsComparator());
	
	private Map<List<String>, List<Function>> group2FunctionMap = new HashMap<>(); //new GroupbyFieldsComparator());
	
	public GroupbyBucket(List<AggregateFunctionType> types){
		this.types = types;
	}
	
	public void addDatapoint(List<String> groupbyFieldValues, List<Double> values){
		// LOG.info("DEBUG: addDatapoint: groupby=["+StringUtils.join(groupbyFieldValues,",")+"], values=["+StringUtils.join(values, ",")+"]");
		
		// locate groupby bucket
		List<Function> functions = group2FunctionMap.get(groupbyFieldValues);
		if(functions == null){
			functions = new ArrayList<Function>();
			for(AggregateFunctionType type : types){
				functions.add(_functionFactories.get(type.name()).createFunction());
			}
			group2FunctionMap.put(groupbyFieldValues, functions);
		}
		int functionIndex = 0;
		for(Double v : values){
			functions.get(functionIndex).run(v);
			functionIndex++;
		}
	}
	
	public Map<List<String>, List<Double>> result(){
		Map<List<String>, List<Double>> result = new HashMap<List<String>, List<Double>>();
		for(Map.Entry<List<String>, List<Function>> entry : this.group2FunctionMap.entrySet()){
			List<Double> values = new ArrayList<Double>();
			for(Function f : entry.getValue()){
				values.add(f.result());
			}
			result.put(entry.getKey(), values);
		}
		return result;
	}

	public List<GroupbyKeyValue> getGroupbyKeyValue(){
		List<GroupbyKeyValue>  results = new ArrayList<GroupbyKeyValue>();
		
		for(Map.Entry<List<String>, List<Function>> entry : this.group2FunctionMap.entrySet()){
			GroupbyKey key = new GroupbyKey();
			for(String keyStr:entry.getKey()){
				try {
					key.addValue(keyStr.getBytes(QueryConstants.CHARSET));
				} catch (UnsupportedEncodingException e) {
					LOG.error(e.getMessage(),e);
				}
			}
			GroupbyValue value = new GroupbyValue();
			for(Function f : entry.getValue()){
				value.add(f.result());
				value.addMeta(f.count());
			}
			results.add(new GroupbyKeyValue(key,value));
		}
		
		return results;
	}
	
	public static interface FunctionFactory{
		public Function createFunction();
	}
	
	public static abstract class Function{
		protected int count;

		public abstract void run(double v);
		public abstract double result();
		public int count(){
			return count;
		}
		public void incrCount(){
			count ++;
		}
	}

	private static class CountFactory implements FunctionFactory{
		@Override
		public Function createFunction(){
			return new Count();
		}
	}
	
	
	private static class Count extends Sum{
		public Count(){
			super();
		}
	}
	
	private static class SumFactory implements FunctionFactory{
		@Override
		public Function createFunction(){
			return new Sum();
		}
	}
	
	private static class Sum extends Function{
		private double summary;
		public Sum(){
			this.summary = 0.0;
		}
		@Override
		public void run(double v){
			this.incrCount();
			this.summary += v;
		}
		
		@Override
		public double result(){
			return this.summary;
		}
	}
	
	private static class MinFactory implements FunctionFactory{
		@Override
		public Function createFunction(){
			return new Min();
		}
	}
	public static class Min extends Function{
		private double minimum;
		public Min(){
			// TODO is this a bug, or only positive numeric calculation is supported
			this.minimum = Double.MAX_VALUE;
		}

		@Override
		public void run(double v){
			if(v < minimum){
				minimum = v;
			}
			this.incrCount();
		}
		
		@Override
		public double result(){
			return minimum;
		}
	}
	
	private static class MaxFactory implements FunctionFactory{
		@Override
		public Function createFunction(){
			return new Max();
		}
	}
	public static class Max extends Function{
		private double maximum;
		public Max(){
			// TODO is this a bug, or only positive numeric calculation is supported
			this.maximum = 0.0;
		}
		@Override
		public void run(double v){
			if(v > maximum){
				maximum = v;
			}
			this.incrCount();
		}
		
		@Override
		public double result(){
			return maximum;
		}
	}
	
	private static class AvgFactory implements FunctionFactory{
		@Override
		public Function createFunction(){
			return new Avg();
		}
	}
	public static class Avg extends Function{
		private double total;
		public Avg(){
			this.total = 0.0;
		}
		@Override
		public void run(double v){
			total += v;
			this.incrCount();
		}
		@Override
		public double result(){
			return this.total/this.count;
		}
	}
}