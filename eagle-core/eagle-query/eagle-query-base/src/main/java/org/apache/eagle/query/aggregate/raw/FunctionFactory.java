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

import org.apache.eagle.query.aggregate.AggregateFunctionType;

import java.util.HashMap;
import java.util.Map;

public abstract class FunctionFactory{
	public abstract Function createFunction();

	public static class AvgFactory extends FunctionFactory {
		@Override
		public Function createFunction(){
			return new Function.Avg();
		}
	}

	public static class MaxFactory extends FunctionFactory {
		@Override
		public Function createFunction(){
			return new Function.Max();
		}
	}

	public static class MinFactory extends FunctionFactory {
		@Override
		public Function createFunction(){
			return new Function.Min();
		}
	}

	public static class CountFactory extends FunctionFactory {
		@Override
		public Function createFunction(){
			return new Function.Count();
		}
	}

	public static class SumFactory extends FunctionFactory {
		@Override
		public Function createFunction(){
			return new Function.Sum();
		}
	}

	public static FunctionFactory locateFunctionFactory(AggregateFunctionType funcType){
		return _functionFactories.get(funcType.name());
	}

	private static Map<String, FunctionFactory> _functionFactories = new HashMap<String, FunctionFactory>();
	static{
		_functionFactories.put(AggregateFunctionType.count.name(), new CountFactory());
		_functionFactories.put(AggregateFunctionType.sum.name(), new SumFactory());
		_functionFactories.put(AggregateFunctionType.min.name(), new MinFactory());
		_functionFactories.put(AggregateFunctionType.max.name(), new MaxFactory());
		_functionFactories.put(AggregateFunctionType.avg.name(), new AvgFactory());
	}
}
	