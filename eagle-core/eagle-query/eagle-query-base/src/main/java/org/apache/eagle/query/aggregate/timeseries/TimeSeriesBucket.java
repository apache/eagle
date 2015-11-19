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

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * only numeric aggregation is supported and number type supported is double
 */
public class TimeSeriesBucket {
	private final static Logger LOG = LoggerFactory.getLogger(TimeSeriesBucket.class);
	private long startTime;
	private long endTime;
	private long interval;
	
	// map of aggregation function to aggregated values 
	List<double[]> aggregatedValues = new ArrayList<double[]>();
	
	// align from the startTime
	/**
	 * 
	 * @param startTime milliseconds
	 * @param endTime milliseconds
	 * @param intervalMillseconds
	 * @param aggFunctions
	 */
	public TimeSeriesBucket(long startTime, long endTime, long intervalms, int numAggFunctions){
		int count =(int)((endTime-startTime)/intervalms);
		for(int i=0; i<numAggFunctions; i++){
			aggregatedValues.add(new double[count]);
		}
	}
	
	/**
	 * add datapoint which has a list of values for different aggregate functions
	 * for example, sum(numHosts), count(*), avg(timespan) etc
	 * @param timestamp
	 * @param values
	 */
	public void addDataPoint(long timestamp, List<Double> values){
		// locate timeseries bucket
		if(timestamp < startTime || timestamp > endTime){
			LOG.warn("timestamp<startTime or timestamp>endTime, ignore this datapoint." + timestamp + "," + startTime + ":" + endTime);
			return;
		}
		int located =(int)((timestamp - startTime)/interval);
		int index = 0;
		for(Double src : values){
			double[] timeSeriesValues = aggregatedValues.get(index);
			timeSeriesValues[located] += src;
			index++;
		}
	}
	
	public List<double[]> aggregatedValues(){
		return this.aggregatedValues;
	}
}
