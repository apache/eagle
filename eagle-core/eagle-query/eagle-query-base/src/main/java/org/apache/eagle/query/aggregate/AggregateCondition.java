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

import java.io.Serializable;
import java.util.List;

/**
 *
 * @since : 11/7/14,2014
 */
public class AggregateCondition implements Serializable{
	private static final long serialVersionUID = 1L;
	private List<String> groupbyFields;
	private List<AggregateFunctionType> aggregateFunctionTypes;
	private List<String> aggregateFields;
	private boolean timeSeries;
	private long intervalMS;

	public List<String> getGroupbyFields() {
		return groupbyFields;
	}

	public void setGroupbyFields(List<String> groupbyFields) {
		this.groupbyFields = groupbyFields;
	}

	public List<AggregateFunctionType> getAggregateFunctionTypes() {
		return aggregateFunctionTypes;
	}

	public void setAggregateFunctionTypes(List<AggregateFunctionType> aggregateFunctionTypes) {
		this.aggregateFunctionTypes = aggregateFunctionTypes;
	}

	public List<String> getAggregateFields() {
		return aggregateFields;
	}

	public void setAggregateFields(List<String> aggregateFields) {
		this.aggregateFields = aggregateFields;
	}

	public boolean isTimeSeries() {
		return timeSeries;
	}

	public void setTimeSeries(boolean timeSeries) {
		this.timeSeries = timeSeries;
	}

	public long getIntervalMS() {
		return intervalMS;
	}

	public void setIntervalMS(long intervalMS) {
		this.intervalMS = intervalMS;
	}
}
