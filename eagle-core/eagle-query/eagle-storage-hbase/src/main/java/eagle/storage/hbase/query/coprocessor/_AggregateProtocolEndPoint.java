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
//package eagle.storage.hbase.query.coprocessor;
//
//import eagle.log.entity.meta.EntityDefinition;
//import eagle.query.aggregate.AggregateFunctionType;
//import eagle.query.aggregate.raw.GroupbyKeyValue;
//import eagle.query.aggregate.raw.RawAggregator;
//import eagle.query.aggregate.timeseries.TimeSeriesAggregator;
//import eagle.storage.hbase.query.coprocessor.generated.AggregateProtos;
//import eagle.storage.hbase.query.coprocessor.impl.AbstractAggregateEndPoint;
//import hadoop.eagle.common.DateTimeUtil;
//import com.google.protobuf.RpcCallback;
//import com.google.protobuf.RpcController;
//import org.apache.hadoop.hbase.client.Scan;
//import org.apache.hadoop.hbase.protobuf.ResponseConverter;
//import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
//import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import java.io.IOException;
//import java.util.List;
//
///**
// * Coprocessor EndPoint of protocol <code>AggregateProtocol</code>
// *
// * <br/>
// * <h2>Deployment:</h2>
// *
// * Firstly deploy jar files to cluster on local file system or HDFS.<br/><br/>
// * Secondly configure in <code>hbase-site.xml</code> as following:
// * <pre>
// * &lt;property&gt;
// *   &lt;name>hbase.coprocessor.region.classes&lt;/name&gt;
// *   &lt;value>AggregateProtocolEndPoint&lt;/value&gt;
// * &lt;/property&gt;
// * </pre>
// * Or register on related hbase tables
// * <pre>
// * hbase(main):005:0>  alter 't1', METHOD => 'table_att', 'coprocessor'=>'hdfs:///foo.jar|AggregateProtocolEndPoint|1001|'
// * </pre>
// *
// * <h2>Reference:</h2>
// * <a href="https://blogs.apache.org/hbase/entry/coprocessor_introduction">
// * 	Coprocessor Introduction
// * 	(Authors: Trend Micro Hadoop Group: Mingjie Lai, Eugene Koontz, Andrew Purtell)
// * </a> <br/><br/>
// *
// * @see AggregateProtocol
// *
//// * @since : 10/31/14,2014
// */
//@SuppressWarnings("unused")
//public class AggregateProtocolEndPoint extends AbstractAggregateEndPoint {
//	private final static Logger LOG = LoggerFactory.getLogger(AggregateProtocolEndPoint.class);
//	/**
//	 *
//	 * @param entityDefinition
//	 * @param scan
//	 * @param groupbyFields
//	 * @param aggregateFuncTypes
//	 * @param aggregatedFields
//	 * @return
//	 * @throws Exception
//	 */
//	@Override
//	public AggregateResult aggregate(EntityDefinition entityDefinition, Scan scan, List<String> groupbyFields, List<byte[]> aggregateFuncTypes, List<String> aggregatedFields) throws IOException {
////		LOG.info("Using coprocessor instance: "+this);
//		checkNotNull(entityDefinition, "entityDefinition");
//		String serviceName = entityDefinition.getService();
//		LOG.info(this.getLogHeader() +" raw group aggregate on service: " + serviceName + " by: " + groupbyFields + " func: " + AggregateFunctionType.fromBytesList(aggregateFuncTypes) + " fields: " + aggregatedFields);
//		if(LOG.isDebugEnabled()) LOG.debug("SCAN: "+scan.toJSON());
//		long _start = System.currentTimeMillis();
//		final RawAggregator aggregator = new RawAggregator(groupbyFields,AggregateFunctionType.fromBytesList(aggregateFuncTypes),aggregatedFields,entityDefinition);
//		InternalReadReport report = this.asyncStreamRead(entityDefinition, scan, aggregator);
//
//		List<GroupbyKeyValue> keyValues = aggregator.getGroupbyKeyValues();
//		AggregateResult result = new AggregateResult();
//		result.setKeyValues(keyValues);
//		result.setStartTimestamp(report.getStartTimestamp());
//		result.setStopTimestamp(report.getStopTimestamp());
//
//		long _stop = System.currentTimeMillis();
//		LOG.info(String.format("%s: scan = %d rows, group = %d keys, startTime = %d, endTime = %d, spend = %d ms", this.getLogHeader(),report.getCounter(),keyValues.size(),report.getStartTimestamp(),report.getStopTimestamp(),(_stop - _start)));
//
//		return result;
//	}
//
//	/**
//	 * TODO: refactor time series aggregator to remove dependency of business logic entity class
//	 *
//	 * @param entityDefinition
//	 * @param scan
//	 * @param groupbyFields
//	 * @param aggregateFuncTypes
//	 * @param aggregatedFields
//	 * @param intervalMin
//	 * @return
//	 * @throws Exception
//	 */
//	@Override
//	public AggregateResult aggregate(EntityDefinition entityDefinition, Scan scan, List<String> groupbyFields, List<byte[]> aggregateFuncTypes, List<String> aggregatedFields, long startTime,long endTime,long intervalMin) throws IOException {
////		LOG.info("Using coprocessor instance: "+this);
//		checkNotNull(entityDefinition, "entityDefinition");
//		String serviceName = entityDefinition.getService();
//		LOG.info(this.getLogHeader() + " time series group aggregate on service: " + serviceName + " by: " + groupbyFields + " func: " + AggregateFunctionType.fromBytesList(aggregateFuncTypes) + " fields: " + aggregatedFields + " intervalMin: " + intervalMin +
//				" from: " + DateTimeUtil.millisecondsToHumanDateWithMilliseconds(startTime) + " to: " + DateTimeUtil.millisecondsToHumanDateWithMilliseconds(endTime));
//		if(LOG.isDebugEnabled()) LOG.debug("SCAN: "+scan.toJSON());
//		long _start = System.currentTimeMillis();
//		final TimeSeriesAggregator aggregator = new TimeSeriesAggregator(groupbyFields,AggregateFunctionType.fromBytesList(aggregateFuncTypes),aggregatedFields,startTime,endTime,intervalMin);
//		InternalReadReport report = this.asyncStreamRead(entityDefinition, scan,aggregator);
//		List<GroupbyKeyValue> keyValues = aggregator.getGroupbyKeyValues();
//
//		AggregateResult result = new AggregateResult();
//		result.setKeyValues(keyValues);
//		result.setStartTimestamp(report.getStartTimestamp());
//		result.setStopTimestamp(report.getStopTimestamp());
//
//		long _stop = System.currentTimeMillis();
//		LOG.info(String.format("%s: scan = %d rows, group = %d keys, startTime = %d, endTime = %d, spend = %d ms", this.getLogHeader(),report.getCounter(),keyValues.size(),report.getStartTimestamp(),report.getStopTimestamp(),(_stop - _start)));
//
//		return result;
//	}
//}