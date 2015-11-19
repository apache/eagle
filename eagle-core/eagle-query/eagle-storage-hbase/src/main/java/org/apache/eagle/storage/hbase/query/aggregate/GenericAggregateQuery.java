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
package org.apache.eagle.storage.hbase.query.aggregate;

import org.apache.eagle.log.entity.GenericMetricEntity;
import org.apache.eagle.log.entity.HBaseInternalLogHelper;
import org.apache.eagle.log.entity.SearchCondition;
import org.apache.eagle.log.entity.meta.EntityConstants;
import org.apache.eagle.log.entity.meta.EntityDefinition;
import org.apache.eagle.log.entity.meta.EntityDefinitionManager;
import org.apache.eagle.query.GenericQuery;
import org.apache.eagle.query.QueryConstants;
import org.apache.eagle.query.aggregate.AggregateCondition;
import org.apache.eagle.query.aggregate.AggregateFunctionType;
import org.apache.eagle.query.aggregate.raw.GroupbyKey;
import org.apache.eagle.query.aggregate.raw.GroupbyKeyValue;
import org.apache.eagle.query.aggregate.raw.GroupbyValue;
import org.apache.eagle.query.aggregate.timeseries.PostFlatAggregateSort;
import org.apache.eagle.query.aggregate.timeseries.SortOption;
import org.apache.eagle.query.aggregate.timeseries.TimeSeriesAggregator;
import org.apache.eagle.query.aggregate.timeseries.TimeSeriesPostFlatAggregateSort;
import org.apache.eagle.storage.hbase.query.coprocessor.AggregateResult;
import org.apache.eagle.storage.hbase.query.coprocessor.impl.AggregateResultCallbackImpl;
import org.apache.eagle.common.DateTimeUtil;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.*;

/**
 * AggregateQuery
 *
 * <ol>
 *   <li>Open HBase connection</li>
 *   <li>Aggregate through Coprocessor</li>
 *   <li>Build GroupAggregateQuery.GroupAggregateQueryReader to process result and order as sort options</li>
 *   <li>Return result list</li>
 * </ol>
 *
 * @since : 11/7/14,2014
 */
public class GenericAggregateQuery implements GenericQuery {
	private static final Logger LOG = LoggerFactory.getLogger(GenericAggregateQuery.class);
	private final List<AggregateFunctionType> sortFuncs;
	private final List<String> sortFields;

	private EntityDefinition entityDef;
	private SearchCondition searchCondition;
	private AggregateCondition aggregateCondition;
	private String prefix;
	private long lastTimestamp = 0;
	private long firstTimestamp = 0;
	private List<SortOption> sortOptions;
	private int top;

	private int aggFuncNum;
	private int sortAggFuncNum;
	private int sortFuncNum;

	/**
	 *
	 * @param serviceName
	 * @param condition
	 * @param aggregateCondition
	 * @param metricName
	 * @throws InstantiationException
	 * @throws IllegalAccessException
	 */
	public GenericAggregateQuery(String serviceName, SearchCondition condition, AggregateCondition aggregateCondition, String metricName)
			throws InstantiationException, IllegalAccessException{
		this(serviceName, condition, aggregateCondition, metricName,null,null,null,0);
	}

	/**
	 *
	 * @param serviceName
	 * @param condition
	 * @param aggregateCondition
	 * @param metricName
	 * @param sortOptions
	 * @param sortFunctionTypes
	 * @param sortFields
	 * @param top
	 * @throws InstantiationException
	 * @throws IllegalAccessException
	 */
	public GenericAggregateQuery(String serviceName, SearchCondition condition,
	                           AggregateCondition aggregateCondition, String metricName,
	                           List<SortOption> sortOptions,List<AggregateFunctionType> sortFunctionTypes,List<String> sortFields,int top)
			throws InstantiationException, IllegalAccessException{
		checkNotNull(serviceName, "serviceName");
		this.searchCondition = condition;
		this.entityDef = EntityDefinitionManager.getEntityByServiceName(serviceName);
		checkNotNull(entityDef, "EntityDefinition");
		checkNotNull(entityDef, "GroupAggregateCondition");
		this.aggregateCondition = aggregateCondition;
		this.aggFuncNum = this.aggregateCondition.getAggregateFunctionTypes().size();
		this.sortOptions = sortOptions;
		this.sortFuncs  = sortFunctionTypes;
		this.sortFuncNum = this.sortOptions == null ? 0: this.sortOptions.size();
		this.sortFields = sortFields;
		this.top = top;

		if(serviceName.equals(GenericMetricEntity.GENERIC_METRIC_SERVICE)){
			if(LOG.isDebugEnabled()) LOG.debug("list metric aggregate query");
			if(metricName == null || metricName.isEmpty()){
				throw new IllegalArgumentException("metricName should not be empty for metric list query");
			}
			if(!condition.getOutputFields().contains(GenericMetricEntity.VALUE_FIELD)){
				condition.getOutputFields().add(GenericMetricEntity.VALUE_FIELD);
			}
			this.prefix = metricName;
		}else{
			if(LOG.isDebugEnabled()) LOG.debug("list entity aggregate query");
			this.prefix = entityDef.getPrefix();
		}

		// Add sort oriented aggregation functions into aggregateCondtion
		if(this.sortOptions!=null){
			// if sort for time series aggregation
			if(this.aggregateCondition.isTimeSeries()) {
				this.sortAggFuncNum = 0;
				int index = 0;
				for (SortOption sortOption : this.sortOptions) {
					if (!sortOption.isInGroupby()) {
						if (LOG.isDebugEnabled())
							LOG.debug("Add additional aggregation functions for sort options " + sortOption.toString() + " in index: " + (this.aggFuncNum + this.sortAggFuncNum));
						AggregateFunctionType _sortFunc = this.sortFuncs.get(index);
						if (AggregateFunctionType.avg.equals(_sortFunc)) {
							this.aggregateCondition.getAggregateFunctionTypes().add(AggregateFunctionType.sum);
						} else {
							this.aggregateCondition.getAggregateFunctionTypes().add(_sortFunc);
						}
						this.aggregateCondition.getAggregateFields().add(this.sortFields.get(index));

						sortOption.setIndex(this.sortAggFuncNum);
						sortAggFuncNum++;
					}
					index++;
				}
			}
		}
	}


	private void checkNotNull(Object o, String message){
		if(o == null){
			throw new IllegalArgumentException(message + " should not be null");
		}
	}

	/**
	 * TODO: Return List<GroupAggregateAPIEntity>
	 *
	 * @see GenericAggregateQuery.TimeSeriesGroupAggregateQueryReader#result()
	 * @see GenericAggregateQuery.FlatGroupAggregateQueryReader#result()
	 *
 	 */
	@Override
	@SuppressWarnings("raw")
	public List result() throws Exception {
		Date start = null;
		Date end = null;
		// shortcut to avoid read when pageSize=0
		if(searchCondition.getPageSize() <= 0){
			return null;
		}
		// Process the time range if needed
		if(entityDef.isTimeSeries()){
			start = DateTimeUtil.humanDateToDate(searchCondition.getStartTime());
			end = DateTimeUtil.humanDateToDate(searchCondition.getEndTime());
		}else{
			start = DateTimeUtil.humanDateToDate(EntityConstants.FIXED_READ_START_HUMANTIME);
			end = DateTimeUtil.humanDateToDate(EntityConstants.FIXED_READ_END_HUMANTIME);
		}
		// Generate the output qualifiers
		final byte[][] outputQualifiers = HBaseInternalLogHelper.getOutputQualifiers(entityDef, searchCondition.getOutputFields());
		GenericAggregateReader reader = new GenericAggregateReader(entityDef,
				searchCondition.getPartitionValues(),
				start, end, searchCondition.getFilter(), searchCondition.getStartRowkey(), outputQualifiers, this.prefix,this.aggregateCondition);
		try{
			if(LOG.isDebugEnabled()) LOG.debug("open and read group aggregate reader");
			reader.open();
			List result = buildGroupAggregateQueryReader(reader,this.aggregateCondition.isTimeSeries()).result();
			if(result == null) throw new IOException("result is null");
			this.firstTimestamp = reader.getFirstTimestamp();
			this.lastTimestamp = reader.getLastTimestamp();
			if(LOG.isDebugEnabled()) LOG.debug("finish read aggregated " + result.size() + " rows");
			return result;
		}catch (IOException ex){
			LOG.error("Fail reading aggregated results", ex);
			throw ex;
		}finally{
			if(reader != null) {
				if(LOG.isDebugEnabled()) LOG.debug("Release HBase connection");
				reader.close();
			}
		}
	}

	///////////////////////////////////////////////////////////
	// GroupAggregateQueryReader(GroupAggregateLogReader)
	// 	|_ FlatGroupAggregateQueryReader
	// 	|_ TimeSeriesGroupAggregateQueryReader
	///////////////////////////////////////////////////////////

	/**
	 * Factory method for {@link GroupAggregateQueryReader}
	 * <pre>
	 * {@link GroupAggregateQueryReader}
	 * |_ {@link FlatGroupAggregateQueryReader}
	 * |_ {@link TimeSeriesGroupAggregateQueryReader}
	 * </pre>
	 * @param reader
	 * @param isTimeSeries
	 * @return
	 * @throws IOException
	 */
	private  GroupAggregateQueryReader  buildGroupAggregateQueryReader(GenericAggregateReader reader,boolean isTimeSeries) throws IOException{
		if(isTimeSeries){
			return new TimeSeriesGroupAggregateQueryReader(reader,this);
		}else{
			return new FlatGroupAggregateQueryReader(reader,this);
		}
	}

	private abstract class GroupAggregateQueryReader {
		protected final GenericAggregateReader reader;
		protected final GenericAggregateQuery query;

		public GroupAggregateQueryReader(GenericAggregateReader reader, GenericAggregateQuery query){
			this.reader = reader;
			this.query = query;
		}
		public abstract <T> List<T> result() throws Exception;

		protected Map<List<String>, List<Double>> keyValuesToMap(List<GroupbyKeyValue> entities) throws Exception {
			Map<List<String>, List<Double>> aggResultMap = new HashMap<List<String>, List<Double>>();
			try {
				for(GroupbyKeyValue keyValue:entities){
					List<String> key = new ArrayList<String>();
					for(BytesWritable bw:keyValue.getKey().getValue()){
						key.add(new String(bw.copyBytes(), QueryConstants.CHARSET));
					}
					List<Double> value = new ArrayList<Double>();
					for(DoubleWritable wa:keyValue.getValue().getValue()){
						value.add(wa.get());
					}
					aggResultMap.put(key, value);
				}
			} catch (UnsupportedEncodingException e) {
				LOG.error(QueryConstants.CHARSET +" not support: "+e.getMessage(),e);
			}
			return aggResultMap;
		}
	}

	private class FlatGroupAggregateQueryReader extends GroupAggregateQueryReader{
		public FlatGroupAggregateQueryReader(GenericAggregateReader reader, GenericAggregateQuery query) {
			super(reader,query);
		}
		@Override
		public List<Map.Entry<List<String>, List<Double>>> result() throws Exception {
			Map<List<String>, List<Double>> aggResultMap = this.keyValuesToMap(this.reader.read());
			if(this.query.sortOptions == null)
				return new ArrayList<Map.Entry<List<String>, List<Double>>>(aggResultMap.entrySet());
			if(LOG.isDebugEnabled()) LOG.debug("Flat sorting");
			return PostFlatAggregateSort.sort(aggResultMap, this.query.sortOptions, this.query.top);
		}
	}

	private class TimeSeriesGroupAggregateQueryReader extends GroupAggregateQueryReader{
		private final Date start;
		private final Date end;
		private final int pointsNum;
		private final int aggFuncNum;
		private final List<SortOption> sortOptions;
		private final List<AggregateFunctionType> sortFuncs;
		private final int sortAggFuncNum;

		public TimeSeriesGroupAggregateQueryReader(GenericAggregateReader reader, GenericAggregateQuery query) throws IOException {
			super(reader,query);
			try {
				if(entityDef.isTimeSeries()){
						this.start = DateTimeUtil.humanDateToDate(searchCondition.getStartTime());
					this.end = DateTimeUtil.humanDateToDate(searchCondition.getEndTime());
				}else{
					start = DateTimeUtil.humanDateToDate(EntityConstants.FIXED_READ_START_HUMANTIME);
					end = DateTimeUtil.humanDateToDate(EntityConstants.FIXED_READ_END_HUMANTIME);
				}
				this.pointsNum = (int)((end.getTime()-1-start.getTime())/this.query.aggregateCondition.getIntervalMS() + 1);
				this.aggFuncNum = this.query.aggFuncNum;
				this.sortOptions = this.query.sortOptions;
				this.sortFuncs = this.query.sortFuncs;
				this.sortAggFuncNum = this.query.sortAggFuncNum;
			} catch (Exception e) {
				throw new IOException(e);
			}
		}

		/**
		 * <h2>TimeSeriesReader result</h2>
		 * <ol>
		 *  <li>generateTimeSeriesDataPoints()</li>
		 *  <li>if not sort options, return generate time series data points</li>
		 *  <li>if requiring sort, sort time series data points by order of flat aggregation</li>
		 * </ol>
		 *
		 * 	<h2>Time Series Sort Algorithms</h2>
		 * 	<ol>
		 *	<li>Flat aggregate on grouped fields without time series bucket index</li>
		 *	<li>Flat aggregated result according given sortOptions</li>
		 *	<li>Sort Time Series Result according the same order of flat aggregated keys</li>
		 * </ol>
		 *
		 * @see #convertToTimeSeriesDataPoints(java.util.List)
		 *
		 * @return
		 * @throws Exception
		 */
		@Override
		public List<Map.Entry<List<String>, List<double[]>>> result() throws Exception {
			List<GroupbyKeyValue> result = this.reader.read();

			// aggregated data points only
			Map<List<String>,List<double[]>> timeseriesDataPoints = convertToTimeSeriesDataPoints(result);

			if(this.query.sortOptions == null)
				// return time-series data points without sort
				return new ArrayList<Map.Entry<List<String>, List<double[]>>>(timeseriesDataPoints.entrySet());

			LOG.info("Time series sorting");

			// Time Series Sort Steps
			// ======================
			// 1. Flat aggregate on grouped fields without time series bucket index
			// 2. Flat aggregated result according given sortOptions
			// 3. Sort Time Series Result according flat aggregated keys' order

			// 1. Flat aggregate on grouped fields without time series bucket index
			AggregateResultCallbackImpl callback = new AggregateResultCallbackImpl(this.sortFuncs);
			for(GroupbyKeyValue kv:result){
				ArrayList<BytesWritable> copykey = new ArrayList<BytesWritable>(kv.getKey().getValue());
				// remove time series bucket index
				copykey.remove(copykey.size()-1);
				GroupbyKey key = new GroupbyKey();

				// [this.aggFuncNum,this.aggFuncNum + this.sortFuncNum)
				GroupbyValue value = new GroupbyValue();
				for(int i = this.aggFuncNum;i<this.aggFuncNum+this.sortAggFuncNum;i++){
					value.add(kv.getValue().get(i));
					value.addMeta(kv.getValue().getMeta(i));
				}
				key.addAll(copykey);
				GroupbyKeyValue keyValue = new GroupbyKeyValue(key,value);
				callback.update(keyValue);
			}
			AggregateResult callbackResult = callback.result();
			Map<List<String>, List<Double>> mapForSort = this.keyValuesToMap(callbackResult.getKeyValues());

			// 2. Flat aggregated result according given sortOptions
//			List<Map.Entry<List<String>, List<Double>>> flatSort = PostFlatAggregateSort.sort(mapForSort , this.sortOptions, Integer.MAX_VALUE);
//			mapForSort = new HashMap<List<String>, List<Double>>();
//			for(Map.Entry<List<String>, List<Double>> entry:flatSort){
//				mapForSort.put(entry.getKey(),entry.getValue());
//			}

			// 3. Sort Time Series Result according flat aggregated keys' order
			return TimeSeriesPostFlatAggregateSort.sort(mapForSort,timeseriesDataPoints,this.sortOptions,this.query.top);
		}

		/**
		 * Convert raw GroupbyKeyValue list into time-series data points hash map
		 *
		 * @param result <code>List&lt;GroupbyKeyValue&gt;</code>
		 * @return Map&lt;List&lt;String&gt;,List&lt;double[]&gt;&gt;
		 * @throws Exception
		 */
		private Map<List<String>,List<double[]>> convertToTimeSeriesDataPoints(List<GroupbyKeyValue> result) throws Exception {
			Map<List<String>, List<Double>> aggResultMap = this.keyValuesToMap(result);
			Map<List<String>,List<double[]>> timeseriesDataPoints = TimeSeriesAggregator.toMetric(aggResultMap,this.pointsNum,this.aggFuncNum);
			return timeseriesDataPoints;
		}
	}

	/**
	 * Get last / max timestamp
	 *
	 * @return lastTimestamp
	 */
	@Override
	public long getLastTimestamp() {
		return this.lastTimestamp;
	}

	/**
	 * Get first / min timestamp
	 *
	 * @return firstTimestamp
	 */
	@Override
	public long getFirstTimeStamp() {
		return this.firstTimestamp;
	}
}