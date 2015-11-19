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
package org.apache.eagle.storage.hbase.query.coprocessor.impl;

import org.apache.eagle.log.entity.meta.EntityDefinition;
import org.apache.eagle.query.aggregate.AggregateFunctionType;
import org.apache.eagle.storage.hbase.query.coprocessor.generated.AggregateProtos;
import org.apache.eagle.storage.hbase.query.coprocessor.*;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.ipc.BlockingRpcCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Not thread safe
 *
 * @since : 11/2/14,2014
 */
public class AggregateClientImpl implements AggregateClient {
	private final static Logger LOG = LoggerFactory.getLogger(AggregateClient.class);
	private AggregateResultCallback callback;

	private void checkNotNull(Object obj,String name) {
		if(obj==null) throw new NullPointerException(name+" is null");
	}

	@Override
	public AggregateResult aggregate(final HTableInterface table,
	                                       final EntityDefinition entityDefinition,
	                                       final Scan scan,
	                                       final List<String> groupbyFields,
	                                       final List<AggregateFunctionType> aggregateFuncTypes,
	                                       final List<String> aggregatedFields,
	                                       final boolean timeSeries,
	                                       final long startTime,
	                                       final long endTime,
	                                       final long intervalMin) throws IOException {
		checkNotNull(entityDefinition,"entityDefinition");
		final List<AggregateFunctionType> _aggregateFuncTypes = convertToCoprocessorAggregateFunc(aggregateFuncTypes);
		final List<byte[]> _aggregateFuncTypesBytes = AggregateFunctionType.toBytesList(_aggregateFuncTypes);
//		if(timeSeries) TimeSeriesAggregator.validateTimeRange(startTime,endTime,intervalMin);
		callback = new AggregateResultCallbackImpl(aggregateFuncTypes);
		try{
			if(!LOG.isDebugEnabled()){
				LOG.info("Going to exec coprocessor: "+AggregateProtocol.class.getSimpleName());
			}else{
				LOG.debug("Going to exec coprocessor: "+AggregateProtocol.class.getName());
			}

//			table.coprocessorExec(AggregateProtocol.class,scan.getStartRow(),scan.getStopRow(),new Batch.Call<AggregateProtocol, AggregateResult>(){
//				@Override
//				public AggregateResult call(AggregateProtocol instance) throws IOException {
//					if(timeSeries){
//						return instance.aggregate(entityDefinition, scan, groupbyFields, _aggregateFuncTypesBytes, aggregatedFields,startTime,endTime,intervalMin);
//					}else{
//						return instance.aggregate(entityDefinition, scan, groupbyFields, _aggregateFuncTypesBytes, aggregatedFields);
//					}
//				}
//			},callback);

          table.coprocessorService(AggregateProtos.AggregateProtocol.class, scan.getStartRow(), scan.getStopRow(), new Batch.Call<AggregateProtos.AggregateProtocol, AggregateProtos.AggregateResult>() {
              @Override
              public AggregateProtos.AggregateResult call(AggregateProtos.AggregateProtocol instance) throws IOException {
                  BlockingRpcCallback<AggregateProtos.AggregateResult> rpcCallback = new BlockingRpcCallback<AggregateProtos.AggregateResult>();
                  if(timeSeries){
                      AggregateProtos.TimeSeriesAggregateRequest timeSeriesAggregateRequest = ProtoBufConverter
                              .toPBTimeSeriesRequest(
                                      entityDefinition,
                                      scan,
                                      groupbyFields,
                                      _aggregateFuncTypesBytes,
                                      aggregatedFields,
                                      startTime,
                                      endTime,
                                      intervalMin);
                      instance.timeseriesAggregate(null, timeSeriesAggregateRequest, rpcCallback);
                      return rpcCallback.get();
					}else{
                      AggregateProtos.AggregateRequest aggregateRequest = ProtoBufConverter.toPBRequest(
                                      entityDefinition, scan, groupbyFields, _aggregateFuncTypesBytes, aggregatedFields);
                      instance.aggregate(null, aggregateRequest, rpcCallback);
                      return rpcCallback.get();
					}
              }
          }, callback);
		} catch (Throwable t){
			LOG.error(t.getMessage(),t);
			throw new IOException(t);
		}
		return callback.result();
	}
	
//	@Override
//	public void result(final GroupbyKeyValueCreationListener[] listeners) {
//		callback.asyncRead(Arrays.asList(listeners));
//	}

	@Override
	public AggregateResult  aggregate(HTableInterface table, EntityDefinition entityDefinition, Scan scan, List<String> groupbyFields, List<AggregateFunctionType> aggregateFuncTypes, List<String> aggregatedFields) throws IOException {
		return this.aggregate(table,entityDefinition,scan,groupbyFields,aggregateFuncTypes,aggregatedFields,false,0,0,0);
	}

	/**
	 *
	 * <h4>
	 *   Convert client side funcs to server side funcs, especially for <b>avg</b>
	 * </h4>
	 * <ul>
	 *  <li><b>avg</b>:
	 *    Coprocessor[ <b>&lt;sum,count&gt;</b>] => Callback[(sum<SUB>1</SUB>+sum<SUB>2</SUB>+...+sum<SUB>n</SUB>)/(count<SUB>1</SUB>+count<SUB>2</SUB>+...+count<SUB>n</SUB>)]
	 * </li>
	 * </ul>
	 * @param funcs List&lt;AggregateFunctionType&gt;
	 * @return
	 */
	private List<AggregateFunctionType> convertToCoprocessorAggregateFunc(List<AggregateFunctionType> funcs){
		List<AggregateFunctionType> copy = new ArrayList<AggregateFunctionType>(funcs);
		for(int i=0;i<funcs.size();i++){
			AggregateFunctionType func = copy.get(i);
			if(AggregateFunctionType.avg.equals(func)){
				copy.set(i,AggregateFunctionType.sum);
			}
		}
		return copy;
	}
}