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
package org.apache.eagle.query.aggregate;

import java.util.ArrayList;
import java.util.List;

public class AggregateParams{
	List<String> groupbyFields;
	boolean counting;
	List<String> sumFunctionFields = new ArrayList<String>();
	List<SortFieldOrder> sortFieldOrders = new ArrayList<SortFieldOrder>();
	
	public List<SortFieldOrder> getSortFieldOrders() {
		return sortFieldOrders;
	}
	public void setSortFieldOrders(List<SortFieldOrder> sortFieldOrders) {
		this.sortFieldOrders = sortFieldOrders;
	}
	public List<String> getGroupbyFields() {
		return groupbyFields;
	}
	public void setGroupbyFields(List<String> groupbyFields) {
		this.groupbyFields = groupbyFields;
	}
	public boolean isCounting() {
		return counting;
	}
	public void setCounting(boolean counting) {
		this.counting = counting;
	}
	public List<String> getSumFunctionFields() {
		return sumFunctionFields;
	}
	public void setSumFunctionFields(List<String> sumFunctionFields) {
		this.sumFunctionFields = sumFunctionFields;
	}

	public static class SortFieldOrder{
		public static final String SORT_BY_AGGREGATE_KEY = "key";
		public static final String SORT_BY_COUNT = "count";
		private String field;
		private boolean ascendant;
		
		public SortFieldOrder(String field, boolean ascendant) {
			super();
			this.field = field;
			this.ascendant = ascendant;
		}
		public String getField() {
			return field;
		}
		public void setField(String field) {
			this.field = field;
		}
		public boolean isAscendant() {
			return ascendant;
		}
		public void setAscendant(boolean ascendant) {
			this.ascendant = ascendant;
		} 
	}
}