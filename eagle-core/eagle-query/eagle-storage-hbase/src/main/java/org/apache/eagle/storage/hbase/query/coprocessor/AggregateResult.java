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
package org.apache.eagle.storage.hbase.query.coprocessor;

import org.apache.eagle.query.aggregate.raw.GroupbyKey;
import org.apache.eagle.query.aggregate.raw.GroupbyKeyValue;
import org.apache.eagle.query.aggregate.raw.GroupbyValue;
import org.apache.eagle.query.aggregate.raw.WritableList;
import org.apache.hadoop.io.Writable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.List;

/**
 * Aggregated writable result consist of group-by key-values list and additional meta information
 * 
 * <h2>Schema</h2>
 * <pre>
 * {
 *  keyValues: WritableList&lt;GroupbyKeyValue&gt;,
 *  startTimestamp: long,
 *  stopTimestamp: long
 * }
 * </pre>
 */
public class AggregateResult implements Writable,Serializable{

	private final static Logger LOG = LoggerFactory.getLogger(AggregateResult.class);

	/**
	 * Automatically generated default serialVersionUID
	 */
	private static final long serialVersionUID = 1L;
	
	private final WritableList<GroupbyKeyValue> keyValues;
	
	private long startTimestamp = 0;
	
	public long getStartTimestamp() {
		return startTimestamp;
	}

	public void setStartTimestamp(long startTimestamp) {
		this.startTimestamp = startTimestamp;
	}

	public long getStopTimestamp() {
		return stopTimestamp;
	}

	public void setStopTimestamp(long stopTimestamp) {
		this.stopTimestamp = stopTimestamp;
	}

	public WritableList<GroupbyKeyValue> getKeyValues() {
		return keyValues;
	}
	
	public void setKeyValues(List<GroupbyKeyValue> keyValues){
		this.keyValues.addAll(keyValues);
	}
	
	private long stopTimestamp;
	
	public AggregateResult(){
		this.keyValues = new WritableList<GroupbyKeyValue>(GroupbyKeyValue.class);
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		this.startTimestamp = in.readLong();
		this.stopTimestamp = in.readLong();
		keyValues.readFields(in);
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeLong(this.startTimestamp);
		out.writeLong(this.stopTimestamp);
		keyValues.write(out);
	}


	public static AggregateResult build(List<String[]> keys,List<double[]> values,List<Integer> counts,long startTimestamp,long stopTimestamp){
		if(keys.size() > values.size()){
			throw new IllegalArgumentException("keys' size: "+keys.size()+" not equal with values' size: "+values.size());
		}
		AggregateResult result = new AggregateResult();
		result.setStartTimestamp(startTimestamp);
		result.setStopTimestamp(stopTimestamp);
		WritableList<GroupbyKeyValue> keyValues = new WritableList<GroupbyKeyValue>(GroupbyKeyValue.class,keys.size());

		for(int i=0;i<keys.size();i++) {
			String[] key  = keys.get(i);
			GroupbyKey gkey = new GroupbyKey();
			for (String k : key) {
				gkey.addValue(k.getBytes());
			}
			GroupbyValue gvalue = new GroupbyValue();
			double[] value = values.get(i);
			for(double v:value){
				gvalue.add(v);
				gvalue.addMeta(counts.get(i));
			}
			keyValues.add(new GroupbyKeyValue(gkey, gvalue));
		}
		result.setKeyValues(keyValues);
		return result;
	}
}