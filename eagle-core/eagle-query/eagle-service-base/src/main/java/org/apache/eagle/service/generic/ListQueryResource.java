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
package org.apache.eagle.service.generic;

import org.apache.eagle.common.config.EagleConfigFactory;
import org.apache.eagle.log.base.taggedlog.TaggedLogAPIEntity;
import org.apache.eagle.log.entity.*;
import org.apache.eagle.log.entity.meta.EntityDefinition;
import org.apache.eagle.log.entity.meta.EntityDefinitionManager;
import org.apache.eagle.query.GenericQuery;
import org.apache.eagle.query.ListQueryCompiler;
import org.apache.eagle.service.common.EagleExceptionWrapper;
import org.apache.eagle.storage.hbase.query.GenericQueryBuilder;
import org.apache.eagle.common.DateTimeUtil;
import com.sun.jersey.api.json.JSONWithPadding;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.time.StopWatch;
import org.apache.eagle.query.aggregate.timeseries.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.GenericEntity;
import javax.ws.rs.core.MediaType;
import java.util.*;

@Path("list")
public class ListQueryResource {
	private static final Logger LOG = LoggerFactory.getLogger(ListQueryResource.class);

	/**
	 * Support old interface without verbose parameter
	 */
	public ListQueryAPIResponseEntity listQuery(@QueryParam("query") String query,
												@QueryParam("startTime") String startTime, @QueryParam("endTime") String endTime,
												@QueryParam("pageSize") int pageSize, @QueryParam("startRowkey") String startRowkey,
												@QueryParam("treeAgg") boolean treeAgg, @QueryParam("timeSeries") boolean timeSeries,
												@QueryParam("intervalmin") long intervalmin, @QueryParam("top") int top,
												@QueryParam("filterIfMissing") boolean filterIfMissing,
												@QueryParam("parallel") int parallel,
												@QueryParam("metricName") String metricName){
		return listQuery(query, startTime, endTime, pageSize, startRowkey, treeAgg, timeSeries, intervalmin, top, filterIfMissing, parallel, metricName,true);
	}

	/**
	 * TODO refactor the code structure,  now it's messy
	 * @param query
	 * @param startTime
	 * @param endTime
	 * @param pageSize
	 * @param startRowkey
	 * @param treeAgg
	 * @param timeSeries
	 * @param intervalmin
	 * @return
	 */
	@GET
	@Produces({MediaType.APPLICATION_JSON})
	public ListQueryAPIResponseEntity listQuery(@QueryParam("query") String query, 
			@QueryParam("startTime") String startTime, @QueryParam("endTime") String endTime,
			@QueryParam("pageSize") int pageSize, @QueryParam("startRowkey") String startRowkey, 
			@QueryParam("treeAgg") boolean treeAgg, @QueryParam("timeSeries") boolean timeSeries, 
			@QueryParam("intervalmin") long intervalmin, @QueryParam("top") int top,
			@QueryParam("filterIfMissing") boolean filterIfMissing,
			@QueryParam("parallel") int parallel,
			@QueryParam("metricName") String metricName,
			@QueryParam("verbose") Boolean verbose) {
		if(!EagleConfigFactory.load().isCoprocessorEnabled())
			return listQueryWithoutCoprocessor(query,startTime,endTime,pageSize,startRowkey,treeAgg,timeSeries,intervalmin,top,filterIfMissing,parallel,metricName,verbose);

		StopWatch watch = new StopWatch();
		watch.start();
		ListQueryAPIResponseEntity result = new ListQueryAPIResponseEntity();
		try{
			validateQueryParameters(startRowkey, pageSize);

			// 1. Compile query to parse parameters and HBase Filter
			ListQueryCompiler comp = new ListQueryCompiler(query, filterIfMissing);
			String serviceName = comp.serviceName();
			
			SearchCondition condition = new SearchCondition();
			condition.setOutputVerbose(verbose == null || verbose);
			condition.setOutputAlias(comp.getOutputAlias());
			condition.setFilter(comp.filter());
			condition.setQueryExpression(comp.getQueryExpression());
			if(comp.sortOptions() == null && top > 0) {
				LOG.warn("Parameter \"top\" is only used for sort query! Ignore top parameter this time since it's not a sort query");
			}

			// 2. Initialize partition values if set
			// TODO: For now we don't support one query to query multiple partitions. In future 
			// if partition is defined for the entity, internally We need to spawn multiple
			// queries and send one query for each search condition for each partition
			final List<String[]> partitionValues = comp.getQueryPartitionValues();
			if (partitionValues != null) {
				condition.setPartitionValues(Arrays.asList(partitionValues.get(0)));
			}

			// 3. Set time range if it's timeseries service
			EntityDefinition ed = EntityDefinitionManager.getEntityByServiceName(serviceName);
			if(ed.isTimeSeries()){
				// TODO check timestamp exists for timeseries or topology data
				condition.setStartTime(startTime);
				condition.setEndTime(endTime);
			}

			// 4. Set HBase start scanning rowkey if given
			condition.setStartRowkey(startRowkey);

			// 5. Set page size
			condition.setPageSize(pageSize);

			// 6. Generate output,group-by,aggregated fields
			List<String> outputFields = comp.outputFields();
			List<String> groupbyFields = comp.groupbyFields();
			List<String> aggregateFields = comp.aggregateFields();
			Set<String> filterFields = comp.getFilterFields();

			// Start to generate output fields list {
			condition.setOutputAll(comp.isOutputAll());
			if(outputFields == null) outputFields = new ArrayList<String>();
			if(comp.hasAgg()){
				if(groupbyFields != null) outputFields.addAll(groupbyFields);
				if(aggregateFields != null) outputFields.addAll(aggregateFields);
				if(GenericMetricEntity.GENERIC_METRIC_SERVICE.equals(serviceName) && !outputFields.contains(GenericMetricEntity.VALUE_FIELD)){
					outputFields.add(GenericMetricEntity.VALUE_FIELD);
				}
			}
			if(filterFields!=null) outputFields.addAll(filterFields);
			condition.setOutputFields(outputFields);
			if(comp.isOutputAll()){
				LOG.info("Output fields: ALL");
			}else{
				LOG.info("Output fields: " + StringUtils.join(outputFields, ","));
			}
			// } END

			// 7. Build GenericQuery
			GenericQuery reader = GenericQueryBuilder
									.select(outputFields)
									.from(serviceName, metricName).where(condition)
									.groupBy(
											comp.hasAgg(),
											groupbyFields,
											comp.aggregateFunctionTypes(),
											aggregateFields)
									.timeSeries(timeSeries, intervalmin)
									.treeAgg(treeAgg)
									.orderBy(comp.sortOptions(),comp.sortFunctions(),comp.sortFields()).top(top)
									.parallel(parallel)								
									.build();
			
			// 8. Fill response object
			List entities = reader.result();
			result.setObj(entities);
			result.setTotalResults(entities.size());
			result.setSuccess(true);
			result.setLastTimestamp(reader.getLastTimestamp());
			result.setFirstTimestamp(reader.getFirstTimeStamp());
		}catch(Exception ex){
			LOG.error("Fail executing list query", ex);
			result.setException(EagleExceptionWrapper.wrap(ex));
			result.setSuccess(false);
			return result;
		}finally{
			watch.stop();
			result.setElapsedms(watch.getTime());
		}
		LOG.info("Query done " + watch.getTime() + " ms");
		return result;
	}
	
	/**
	 * <b>TODO</b> remove the legacy deprecated implementation of listQueryWithoutCoprocessor
	 *
	 * @see #listQuery(String, String, String, int, String, boolean, boolean, long, int, boolean, int, String,Boolean)
	 *
	 * @param query
	 * @param startTime
	 * @param endTime
	 * @param pageSize
	 * @param startRowkey
	 * @param treeAgg
	 * @param timeSeries
	 * @param intervalmin
	 * @return
	 */
	@GET
	@Path("/legacy")
	@Produces({MediaType.APPLICATION_JSON})
	@Deprecated
	public ListQueryAPIResponseEntity listQueryWithoutCoprocessor(@QueryParam("query") String query,
	                                                              @QueryParam("startTime") String startTime, @QueryParam("endTime") String endTime,
	                                                              @QueryParam("pageSize") int pageSize, @QueryParam("startRowkey") String startRowkey,
	                                                              @QueryParam("treeAgg") boolean treeAgg, @QueryParam("timeSeries") boolean timeSeries,
	                                                              @QueryParam("intervalmin") long intervalmin, @QueryParam("top") int top,
	                                                              @QueryParam("filterIfMissing") boolean filterIfMissing,
	                                                              @QueryParam("parallel") int parallel,
	                                                              @QueryParam("metricName") String metricName,
																  @QueryParam("verbose") Boolean verbose) {
		StopWatch watch = new StopWatch();
		watch.start();
		ListQueryAPIResponseEntity result = new ListQueryAPIResponseEntity();
		try{
			validateQueryParameters(startRowkey, pageSize);
			ListQueryCompiler comp = new ListQueryCompiler(query, filterIfMissing);
			String serviceName = comp.serviceName();

			SearchCondition condition = new SearchCondition();
			condition.setFilter(comp.filter());
			condition.setQueryExpression(comp.getQueryExpression());
			if(comp.sortOptions() == null && top > 0) {
				LOG.warn("Parameter \"top\" is only used for sort query! Ignore top parameter this time since it's not a sort query");
			}

			// TODO: For now we don't support one query to query multiple partitions. In future
			// if partition is defined for the entity, internally We need to spawn multiple
			// queries and send one query for each search condition for each partition
			final List<String[]> partitionValues = comp.getQueryPartitionValues();
			if (partitionValues != null) {
				condition.setPartitionValues(Arrays.asList(partitionValues.get(0)));
			}
			EntityDefinition ed = EntityDefinitionManager.getEntityByServiceName(serviceName);
			if(ed.isTimeSeries()){
				// TODO check timestamp exists for timeseries or topology data
				condition.setStartTime(startTime);
				condition.setEndTime(endTime);
			}
			condition.setOutputVerbose(verbose==null || verbose );
			condition.setOutputAlias(comp.getOutputAlias());
			condition.setOutputAll(comp.isOutputAll());
			condition.setStartRowkey(startRowkey);
			condition.setPageSize(pageSize);

			List<String> outputFields = comp.outputFields();
			if(outputFields == null) outputFields = new ArrayList<String>();

			/**
			 * TODO ugly logic, waiting for refactoring
			 */
			if(!comp.hasAgg() && !serviceName.equals(GenericMetricEntity.GENERIC_METRIC_SERVICE)){ // pure list query
//				List<String> outputFields = comp.outputFields();
				Set<String> filterFields = comp.getFilterFields();
				if(filterFields != null) outputFields.addAll(filterFields);
				condition.setOutputFields(outputFields);
				if(condition.isOutputAll()){
					LOG.info("Output: ALL");
				}else{
					LOG.info("Output: " + StringUtils.join(condition.getOutputFields(), ", "));
				}
				GenericEntityBatchReader reader = new GenericEntityBatchReader(serviceName, condition);
				List<? extends TaggedLogAPIEntity> entityList = reader.read();
				result.setObj(entityList);
				result.setTotalResults(entityList.size());
				result.setSuccess(true);
				result.setLastTimestamp(reader.getLastTimestamp());
				result.setFirstTimestamp(reader.getFirstTimestamp());
			}else if(!comp.hasAgg() && serviceName.equals(GenericMetricEntity.GENERIC_METRIC_SERVICE)){
				// validate metric name
				if(metricName == null || metricName.isEmpty()){
					throw new IllegalArgumentException("metricName should not be empty for metric list query");
				}
//				List<String> outputFields = comp.outputFields();
				Set<String> filterFields = comp.getFilterFields();
				if(filterFields != null) outputFields.addAll(filterFields);
				condition.setOutputFields(outputFields);
				if(condition.isOutputAll()){
					LOG.info("Output: ALL");
				}else{
					LOG.info("Output: " + StringUtils.join(condition.getOutputFields(), ", "));
				}
				GenericMetricEntityBatchReader reader = new GenericMetricEntityBatchReader(metricName, condition);
				List<? extends TaggedLogAPIEntity> entityList = reader.read();
				result.setObj(entityList);
				result.setTotalResults(entityList.size());
				result.setSuccess(true);
				result.setLastTimestamp(reader.getLastTimestamp());
				result.setFirstTimestamp(reader.getFirstTimestamp());
			}
			else if(!treeAgg && !timeSeries && parallel <= 0 ){ // non time-series based aggregate query, not hierarchical
				List<String> groupbyFields = comp.groupbyFields();
				List<String> aggregateFields = comp.aggregateFields();
				Set<String> filterFields = comp.getFilterFields();
//				List<String> outputFields = new ArrayList<String>();
				if(groupbyFields != null) outputFields.addAll(groupbyFields);
				if(filterFields != null) outputFields.addAll(filterFields);
				outputFields.addAll(aggregateFields);

				if(GenericMetricEntity.GENERIC_METRIC_SERVICE.equals(serviceName) && !outputFields.contains(GenericMetricEntity.VALUE_FIELD)){
					outputFields.add(GenericMetricEntity.VALUE_FIELD);
				}

				FlatAggregator agg = new FlatAggregator(groupbyFields, comp.aggregateFunctionTypes(), comp.aggregateFields());
				StreamReader reader = null;
				if(ed.getMetricDefinition() == null){
					reader = new GenericEntityStreamReader(serviceName, condition);
				}else{ // metric aggregation need metric reader
					reader = new GenericMetricEntityDecompactionStreamReader(metricName, condition);
				}
				condition.setOutputFields(outputFields);
				if(condition.isOutputAll()){
					LOG.info("Output: ALL");
				}else{
					LOG.info("Output: " + StringUtils.join(condition.getOutputFields(), ", "));
				}
				reader.register(agg);
				reader.readAsStream();
				ArrayList<Map.Entry<List<String>, List<Double>>> obj = new ArrayList<Map.Entry<List<String>, List<Double>>>();
				obj.addAll(agg.result().entrySet());
				if(comp.sortOptions() == null){
					result.setObj(obj);
				}else{ // has sort options
					result.setObj(PostFlatAggregateSort.sort(agg.result(), comp.sortOptions(), top));
				}
				result.setTotalResults(0);
				result.setSuccess(true);
				result.setLastTimestamp(reader.getLastTimestamp());
				result.setFirstTimestamp(reader.getFirstTimestamp());
			}else if(!treeAgg && !timeSeries && parallel > 0){ // TODO ugly branch, let us refactor
				List<String> groupbyFields = comp.groupbyFields();
				List<String> aggregateFields = comp.aggregateFields();
				Set<String> filterFields = comp.getFilterFields();
//				List<String> outputFields = new ArrayList<String>();
				if(groupbyFields != null) outputFields.addAll(groupbyFields);
				if(filterFields != null) outputFields.addAll(filterFields);
				outputFields.addAll(aggregateFields);
				if(GenericMetricEntity.GENERIC_METRIC_SERVICE.equals(serviceName) && !outputFields.contains(GenericMetricEntity.VALUE_FIELD)){
					outputFields.add(GenericMetricEntity.VALUE_FIELD);
				}
				condition.setOutputFields(outputFields);
				if(condition.isOutputAll()){
					LOG.info("Output: ALL");
				}else{
					LOG.info("Output: " + StringUtils.join(condition.getOutputFields(), ", "));
				}
				FlatAggregator agg = new FlatAggregator(groupbyFields, comp.aggregateFunctionTypes(), comp.aggregateFields());
				EntityCreationListener listener = EntityCreationListenerFactory.synchronizedEntityCreationListener(agg);
				StreamReader reader = new GenericEntityStreamReaderMT(serviceName, condition, parallel);
				reader.register(listener);
				reader.readAsStream();
				ArrayList<Map.Entry<List<String>, List<Double>>> obj = new ArrayList<Map.Entry<List<String>, List<Double>>>();
				obj.addAll(agg.result().entrySet());
				if(comp.sortOptions() == null){
					result.setObj(obj);
				}else{ // has sort options
					result.setObj(PostFlatAggregateSort.sort(agg.result(), comp.sortOptions(), top));
				}
				result.setTotalResults(0);
				result.setSuccess(true);
				result.setLastTimestamp(reader.getLastTimestamp());
				result.setFirstTimestamp(reader.getFirstTimestamp());
			}else if(!treeAgg && timeSeries){ // time-series based aggregate query, not hierarchical
				List<String> groupbyFields = comp.groupbyFields();
				List<String> sortFields = comp.sortFields();
				List<String> aggregateFields = comp.aggregateFields();
				Set<String> filterFields = comp.getFilterFields();
//				List<String> outputFields = new ArrayList<String>();
				if(groupbyFields != null) outputFields.addAll(groupbyFields);
				if(filterFields != null) outputFields.addAll(filterFields);
				if (sortFields != null) outputFields.addAll(sortFields);
				outputFields.addAll(aggregateFields);
				if(GenericMetricEntity.GENERIC_METRIC_SERVICE.equals(serviceName) && !outputFields.contains(GenericMetricEntity.VALUE_FIELD)){
					outputFields.add(GenericMetricEntity.VALUE_FIELD);
				}
				StreamReader reader = null;
				if(ed.getMetricDefinition() == null){
					if(parallel <= 0){ // TODO ugly quick win
						reader = new GenericEntityStreamReader(serviceName, condition);
					}else{
						reader = new GenericEntityStreamReaderMT(serviceName, condition, parallel);
					}
				}else{ // metric aggregation need metric reader
					reader = new GenericMetricEntityDecompactionStreamReader(metricName, condition);
					if(!outputFields.contains(GenericMetricEntity.VALUE_FIELD)){
						outputFields.add(GenericMetricEntity.VALUE_FIELD);
					}
				}
				condition.setOutputFields(outputFields);
				if(condition.isOutputAll()){
					LOG.info("Output: ALL");
				}else{
					LOG.info("Output: " + StringUtils.join(condition.getOutputFields(), ", "));
				}
				TimeSeriesAggregator tsAgg = new TimeSeriesAggregator(groupbyFields, comp.aggregateFunctionTypes(), aggregateFields,
						DateTimeUtil.humanDateToDate(condition.getStartTime()).getTime(), DateTimeUtil.humanDateToDate(condition.getEndTime()).getTime(), intervalmin*60*1000);
				if(parallel <= 0){
					reader.register(tsAgg);
				}else{
					EntityCreationListener listener = EntityCreationListenerFactory.synchronizedEntityCreationListener(tsAgg);
					reader.register(listener);
				}
				// for sorting
				FlatAggregator sortAgg = null;
				if (comp.sortOptions() != null) {
					sortAgg = new FlatAggregator(groupbyFields, comp.sortFunctions(), comp.sortFields());
					if(parallel <= 0){
						reader.register(sortAgg);
					}else{
						EntityCreationListener listener = EntityCreationListenerFactory.synchronizedEntityCreationListener(sortAgg);
						reader.register(listener);
					}
				}
				reader.readAsStream();
				ArrayList<Map.Entry<List<String>, List<double[]>>> obj = new ArrayList<Map.Entry<List<String>, List<double[]>>>();
				obj.addAll(tsAgg.getMetric().entrySet());
				if(comp.sortOptions() == null){
					result.setObj(obj);
				}else{ // has sort options
					result.setObj(TimeSeriesPostFlatAggregateSort.sort(sortAgg.result(), tsAgg.getMetric(), comp.sortOptions(), top));
				}
				result.setTotalResults(0);
				result.setSuccess(true);
				result.setLastTimestamp(reader.getLastTimestamp());
				result.setFirstTimestamp(reader.getFirstTimestamp());
			}
			else{ // use hierarchical aggregate mode
				List<String> groupbyFields = comp.groupbyFields();
				List<String> aggregateFields = comp.aggregateFields();
				Set<String> filterFields = comp.getFilterFields();
//				List<String> outputFields = new ArrayList<String>();
				if(groupbyFields != null) outputFields.addAll(groupbyFields);
				if(filterFields != null) outputFields.addAll(filterFields);
				outputFields.addAll(aggregateFields);
				if(GenericMetricEntity.GENERIC_METRIC_SERVICE.equals(serviceName) && !outputFields.contains(GenericMetricEntity.VALUE_FIELD)){
					outputFields.add(GenericMetricEntity.VALUE_FIELD);
				}
				condition.setOutputFields(outputFields);
				if(condition.isOutputAll()){
					LOG.info("Output: ALL");
				}else{
					LOG.info("Output: " + StringUtils.join(condition.getOutputFields(), ", "));
				}
				GenericEntityStreamReader reader = new GenericEntityStreamReader(serviceName, condition);
				HierarchicalAggregator agg = new HierarchicalAggregator(groupbyFields, comp.aggregateFunctionTypes(), comp.aggregateFields());
				reader.register(agg);
				reader.readAsStream();
				if(comp.sortOptions() == null){
					result.setObj(agg.result());
				}else{ // has sort options
					result.setObj(PostHierarchicalAggregateSort.sort(agg.result(), comp.sortOptions()));
				}
				result.setTotalResults(0);
				result.setSuccess(true);
				result.setLastTimestamp(reader.getLastTimestamp());
				result.setFirstTimestamp(reader.getFirstTimestamp());
			}
		}catch(Exception ex){
			LOG.error("Fail executing list query: " + query, ex);
			result.setException(EagleExceptionWrapper.wrap(ex));
			result.setSuccess(false);
			return result;
		}finally{
			watch.stop();
			result.setElapsedms(watch.getTime());
		}
		LOG.info("Query done " + watch.getTime() + " ms");
		return result;
	}


	@GET
	@Path("/jsonp")
	@Produces({"application/x-javascript", "application/json", "application/xml"})
	public JSONWithPadding listQueryWithJsonp(@QueryParam("query") String query, 
			@QueryParam("startTime") String startTime, @QueryParam("endTime") String endTime,
			@QueryParam("pageSize") int pageSize, @QueryParam("startRowkey") String startRowkey, 
			@QueryParam("treeAgg") boolean treeAgg, @QueryParam("timeSeries") boolean timeSeries, 
			@QueryParam("intervalmin") long intervalmin, @QueryParam("top") int top,
			@QueryParam("filterIfMissing") boolean filterIfMissing,
			@QueryParam("parallel") int parallel,
			@QueryParam("update") String callback,
			@QueryParam("verbose") boolean verbose) {
		ListQueryAPIResponseEntity result = listQuery(query, startTime, endTime, pageSize, startRowkey, treeAgg, timeSeries, intervalmin, top, filterIfMissing, parallel, null,verbose);
		return new JSONWithPadding(new GenericEntity<ListQueryAPIResponseEntity>(result){}, callback);		
	}
	
	private void validateQueryParameters(String startRowkey, int pageSize){
		if(pageSize <= 0){
			throw new IllegalArgumentException("Positive pageSize value should be always provided. The list query format is:\n" + "eagle-service/rest/list?query=<querystring>&pageSize=10&startRowkey=xyz&startTime=xxx&endTime=xxx");
		}
		
		if(startRowkey != null && startRowkey.equals("null")){
			LOG.warn("startRowkey being null string is not same to startRowkey == null");
		}
		return;
	}
}
