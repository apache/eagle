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

public abstract class Function{
	private int count = 0;
	protected void incrCount(int num){ count += num; }
	public int count(){ return count; }
	public abstract void run(double v,int count);
	public void run(double v){ run(v,1); }
	public abstract double result();

	public static class Avg extends Function {
		private double total;
		public Avg(){
			this.total = 0.0;
		}
		@Override
		public void run(double v,int count){
			this.incrCount(count);
			total += v;
		}
		@Override
		public double result(){
			return this.total/this.count();
		}
	}

	public static class Max extends Function {
		private double maximum;
		public Max(){
			// TODO is this a bug, or only positive numeric calculation is supported
			this.maximum = 0.0;
		}

		@Override
		public void run(double v,int count){
			this.incrCount(count);
			if(v > maximum){
				maximum = v;
			}
		}

		@Override
		public double result(){
			return maximum;
		}
	}

	public static class Min extends Function {
		private double minimum;
		public Min(){
			// TODO is this a bug, or only positive numeric calculation is supported
			this.minimum = Double.MAX_VALUE;
		}
		@Override
		public void run(double v,int count){
			this.incrCount(count);
			if(v < minimum){
				minimum = v;
			}
		}

		@Override
		public double result(){
			return minimum;
		}
	}

	public static class Sum extends Function {
		private double summary;
		public Sum(){
			this.summary = 0.0;
		}
		@Override
		public void run(double v,int count){
			this.incrCount(count);
			this.summary += v;
		}

		@Override
		public double result(){
			return this.summary;
		}
	}

	public static class Count extends Sum{
		public Count(){
			super();
		}
	}
}