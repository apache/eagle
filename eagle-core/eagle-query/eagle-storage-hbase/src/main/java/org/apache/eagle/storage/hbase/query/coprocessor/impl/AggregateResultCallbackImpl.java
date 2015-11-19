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

import org.apache.eagle.query.aggregate.AggregateFunctionType;
import org.apache.eagle.storage.hbase.query.coprocessor.AggregateResult;
import org.apache.eagle.storage.hbase.query.coprocessor.AggregateResultCallback;
import org.apache.eagle.storage.hbase.query.coprocessor.ProtoBufConverter;
import org.apache.eagle.storage.hbase.query.coprocessor.generated.AggregateProtos;
import org.apache.eagle.common.ByteUtil;
import org.apache.eagle.query.aggregate.raw.*;
import org.apache.hadoop.hbase.coprocessor.CoprocessorException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @since : 11/3/14,2014
 */
public class AggregateResultCallbackImpl implements AggregateResultCallback {
	private final static Logger LOG = LoggerFactory.getLogger(AggregateResultCallback.class);
	private Map<GroupbyKey,List<Function>> groupedFuncs = new HashMap<GroupbyKey, List<Function>>();
	private List<FunctionFactory> functionFactories = new ArrayList<FunctionFactory>();
	private int numFuncs = 0;
	private long kvCounter = 0;
	private int regionCounter = 0;
	private long startTimestamp;
	private long stopTimestamp;
	
	// Start RPC call time (i.e constructor initialized time)
	private final long _start;
	
	public AggregateResultCallbackImpl(List<AggregateFunctionType> aggregateFunctionTypes){
		this.numFuncs = aggregateFunctionTypes.size();
		for(AggregateFunctionType type: aggregateFunctionTypes){
			 functionFactories.add(FunctionFactory.locateFunctionFactory(type));
		}
		this._start = System.currentTimeMillis();
	}

//	@Override
	public void update(byte[] region, byte[] row, AggregateResult result) {
		AggregateResult _result = result;
		regionCounter ++;
		kvCounter += _result.getKeyValues().size();
		if(this.startTimestamp == 0 || this.startTimestamp > _result.getStartTimestamp()){
			this.startTimestamp = _result.getStartTimestamp();
		}
		if(this.stopTimestamp == 0 || this.stopTimestamp < _result.getStopTimestamp()){
			this.stopTimestamp = _result.getStopTimestamp();
		}
		for(GroupbyKeyValue keyValue:_result.getKeyValues()){
			update(keyValue);
		}
	}

	public void update(GroupbyKeyValue keyValue) {
		// Incr kvCounter if call #update(GroupbyKeyValue) directly
		// instead of #update(byte[] region, byte[] row, AggregateResult result)
		if(this.getKVCounter() == 0) this.kvCounter ++;
		// Accumulate key value for GroubyKey mapped Functions
		GroupbyKey groupedKey = keyValue.getKey();
		List<Function> funcs = groupedFuncs.get(groupedKey);
		if(funcs==null){
			funcs = new ArrayList<Function>();
			for(FunctionFactory functionFactory:this.functionFactories){
				funcs.add(functionFactory.createFunction());
			}
			groupedFuncs.put(groupedKey, funcs);
		}
		for(int i=0;i<this.numFuncs;i++){
			int intCount = 1;
			byte[] count = keyValue.getValue().getMeta(i).getBytes();
			if(count != null){
				intCount = ByteUtil.bytesToInt(count);
			}
			funcs.get(i).run(keyValue.getValue().get(i).get(), intCount);
		}
	}

	public long getKVCounter(){
		return this.kvCounter;
	}

	public long getRegionCounter(){
		return this.regionCounter;
	}

	public AggregateResult result(){
		List<GroupbyKeyValue> mergedKeyValues = new ArrayList<GroupbyKeyValue>();
		for(Map.Entry<GroupbyKey,List<Function>> entry:this.groupedFuncs.entrySet()){
			GroupbyValue value = new GroupbyValue(this.numFuncs);
			for(Function func:entry.getValue()){
				double _result = func.result();
				int _count = func.count();
				value.add(_result);
				value.addMeta(_count);
			}
			mergedKeyValues.add(new GroupbyKeyValue(entry.getKey(),value));
		}
		
		final long _stop = System.currentTimeMillis();
		if(this.getRegionCounter() > 0) {
			LOG.info(String.format("result = %d rows, startTime = %d, endTime = %d, source = %d rows, regions = %d, , spend = %d ms", mergedKeyValues.size(),this.startTimestamp,this.stopTimestamp, this.getKVCounter(), this.getRegionCounter(),(_stop - _start)));
		}else{
			LOG.info(String.format("result = %d rows, startTime = %d, endTime = %d, source = %d rows, spend = %d ms", mergedKeyValues.size(),this.startTimestamp,this.stopTimestamp,this.getKVCounter(), (_stop - _start)));
		}
		AggregateResult result = new AggregateResult();
		result.setKeyValues(mergedKeyValues);
		result.setStartTimestamp(this.startTimestamp);
		result.setStopTimestamp(this.stopTimestamp);
		return result;
	}

    @Override
    public void update(byte[] region, byte[] row, AggregateProtos.AggregateResult result) {
        try {
            if(result == null) throw new IllegalStateException(new CoprocessorException("result is null"));
            this.update(region,row, ProtoBufConverter.fromPBResult(result));
        } catch (IOException e) {
            LOG.error("Failed to convert PB-Based message",e);
        }
    }
}