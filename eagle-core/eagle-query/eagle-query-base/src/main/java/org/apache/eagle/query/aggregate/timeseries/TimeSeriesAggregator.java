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

import org.apache.eagle.log.base.taggedlog.TaggedLogAPIEntity;
import org.apache.eagle.query.aggregate.AggregateFunctionType;
import org.apache.eagle.query.aggregate.raw.GroupbyKeyAggregatable;
import org.apache.eagle.query.aggregate.raw.GroupbyKeyValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * TODO Assuming that data point comes in the sequence of occurrence time desc or asc would 
 * save memory for holding all the data in the memory
 *
 * <h3>Aggregate Bucket Structure</h3>
 * <pre>
 * {
 *  ["key<SUB>1</SUB>","key<SUB>2</SUB>",...,(entity.getTimestamp() - startTime)/intervalms]:[value<SUB>1</SUB>,value<SUB>2</SUB>,...,value<SUB>n</SUB>]
 * }
 * </pre>
 *
 */
public class TimeSeriesAggregator extends FlatAggregator implements GroupbyKeyAggregatable {
	private final static Logger LOG = LoggerFactory.getLogger(TimeSeriesAggregator.class);
	private static final int DEFAULT_DATAPOINT_MAX_COUNT = 1000;
	private long startTime;
	private long endTime;
	private long intervalms;
	private int numFunctions;
	private int ignoredEntityCounter = 0;
	
	public TimeSeriesAggregator(List<String> groupbyFields, List<AggregateFunctionType> aggregateFuntionTypes, List<String> aggregatedFields,
			long startTime, long endTime, long intervalms){
		super(groupbyFields, aggregateFuntionTypes, aggregatedFields);
		// guard to avoid too many data points returned
//		validateTimeRange(startTime, endTime, intervalms);
		this.startTime = startTime;
		this.endTime = endTime;
		this.intervalms = intervalms;
		this.numFunctions = aggregateFuntionTypes.size();
	}

//	@Deprecated
//	public static void validateTimeRange(long startTime, long endTime, long intervalms){
//		if(startTime >= endTime || intervalms <= 0){
//			throw new IllegalArgumentException("invalid argument, startTime should be less than endTime and interval must be greater than 0, starTime is " + startTime + " and endTime is " + endTime + ", interval is " + intervalms);
//		}
//		if((endTime-startTime)/intervalms > DEFAULT_DATAPOINT_MAX_COUNT){
//			throw new IllegalArgumentException("invalid argument, # of datapoints should be less than " + DEFAULT_DATAPOINT_MAX_COUNT + ", current # of datapoints is " + (endTime-startTime)/intervalms);
//		}
//	}
	
	public void accumulate(TaggedLogAPIEntity entity) throws Exception{
		List<String> groupbyFieldValues = createGroup(entity);
		// TODO: make sure timestamp be in range of this.startTime to this.endTime in outer side
		// guard the time range to avoid to accumulate entities whose timestamp is bigger than endTime
		if(entity.getTimestamp() >= this.endTime || entity.getTimestamp() < this.startTime){
			if(LOG.isDebugEnabled()) LOG.debug("Ignore in-coming entity whose timestamp > endTime or < startTime, timestamp: " + entity.getTimestamp() + ", startTime:" + startTime + ", endTime:" + endTime);
			this.ignoredEntityCounter ++;
			return;
		}
		// time series bucket index
		long located =(entity.getTimestamp() - startTime)/intervalms; 
		groupbyFieldValues.add(String.valueOf(located));
		List<Double> preAggregatedValues = createPreAggregatedValues(entity);
		bucket.addDatapoint(groupbyFieldValues, preAggregatedValues);
	}
	
	public Map<List<String>, List<Double>> result(){
		if(this.ignoredEntityCounter > 0)
			LOG.warn("Ignored "+this.ignoredEntityCounter+" entities for reason: timestamp > "+this.endTime+" or < "+this.startTime);
		return bucket.result();
	}

	/**
	 * Support new aggregate result
	 *
	 * @return
	 */
	@Override
	public List<GroupbyKeyValue> getGroupbyKeyValues(){
		if(this.ignoredEntityCounter > 0)
			LOG.warn("Ignored "+this.ignoredEntityCounter+" entities for reason: timestamp > "+this.endTime+" or < "+this.startTime);
		return bucket.getGroupbyKeyValue();
	}
	
	public Map<List<String>, List<double[]>> getMetric(){
		// groupbyfields+timeseriesbucket --> aggregatedvalues for different function
		Map<List<String>, List<Double>> result = bucket.result();
//		Map<List<String>, List<double[]>> timeseriesDatapoints = new HashMap<List<String>, List<double[]>>();
//		/**
//		 * bug fix: startTime is inclusive and endTime is exclusive
//		 */
////		int numDatapoints =(int)((endTime-startTime)/intervalms + 1);
//		int numDatapoints =(int)((endTime-1-startTime)/intervalms + 1);
//		for(Map.Entry<List<String>, List<Double>> entry : result.entrySet()){
//			// get groups
//			List<String> groupbyFields = entry.getKey();
//			List<String> copy = new ArrayList<String>(groupbyFields);
//			String strTimeseriesIndex = copy.remove(copy.size()-1);
//			List<double[]> functionValues = timeseriesDatapoints.get(copy);
//			if(functionValues == null){
//				functionValues = new ArrayList<double[]>();
//				timeseriesDatapoints.put(copy, functionValues);
//				for(int i=0; i<numFunctions; i++){
//					functionValues.add(new double[numDatapoints]);
//				}
//			}
//			int timeseriesIndex = Integer.valueOf(strTimeseriesIndex);
//			int functionIndex = 0;
//			for(double[] values : functionValues){
//				values[timeseriesIndex] = entry.getValue().get(functionIndex);
//				functionIndex++;
//			}
//		}
//		return timeseriesDatapoints;
		return toMetric(result,(int)((endTime-1-startTime)/intervalms + 1),this.numFunctions);
	}

	public static Map<List<String>, List<double[]>> toMetric(Map<List<String>, List<Double>> result,int numDatapoints,int numFunctions){
		Map<List<String>, List<double[]>> timeseriesDatapoints = new HashMap<List<String>, List<double[]>>();
		/**
		 * bug fix: startTime is inclusive and endTime is exclusive
		 */
//		int numDatapoints =(int)((endTime-startTime)/intervalms + 1);
//		int numDatapoints =(int)((endTime-1-startTime)/intervalms + 1);
		for(Map.Entry<List<String>, List<Double>> entry : result.entrySet()){
			// get groups
			List<String> groupbyFields = entry.getKey();
			List<String> copy = new ArrayList<String>(groupbyFields);
			String strTimeseriesIndex = copy.remove(copy.size()-1);
			List<double[]> functionValues = timeseriesDatapoints.get(copy);
			if(functionValues == null){
				functionValues = new ArrayList<double[]>();
				timeseriesDatapoints.put(copy, functionValues);
				for(int i=0; i<numFunctions; i++){
					functionValues.add(new double[numDatapoints]);
				}
			}
			int timeseriesIndex = Integer.valueOf(strTimeseriesIndex);
			int functionIndex = 0;
			for(double[] values : functionValues){
				values[timeseriesIndex] = entry.getValue().get(functionIndex);
				functionIndex++;
			}
		}
		return timeseriesDatapoints;
	}
}
