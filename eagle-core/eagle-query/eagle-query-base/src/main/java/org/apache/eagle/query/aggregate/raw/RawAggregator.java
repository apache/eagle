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
package org.apache.eagle.query.aggregate.raw;

import org.apache.eagle.log.entity.QualifierCreationListener;
import org.apache.eagle.log.entity.meta.EntityDefinition;
import org.apache.eagle.query.aggregate.AggregateFunctionType;

import java.util.List;
import java.util.ListIterator;
import java.util.Map;

public class RawAggregator implements QualifierCreationListener,GroupbyKeyAggregatable {
	private List<String> groupbyFields;
	private GroupbyKey key;
	private static final byte[] UNASSIGNED = "unassigned".getBytes();
	private RawGroupbyBucket bucket;

	public RawAggregator(List<String> groupbyFields, List<AggregateFunctionType> aggregateFunctionTypes, List<String> aggregatedFields, EntityDefinition ed){
		this.groupbyFields = groupbyFields;
		key = new GroupbyKey();
		bucket = new RawGroupbyBucket(aggregateFunctionTypes, aggregatedFields, ed);
	}

	@Override
	public void qualifierCreated(Map<String, byte[]> qualifiers){
		key.clear();
		ListIterator<String> it = groupbyFields.listIterator();
		while(it.hasNext()){
			byte[] groupbyFieldValue = qualifiers.get(it.next());
			if(groupbyFieldValue == null){
				key.addValue(UNASSIGNED);
			}else{
				key.addValue(groupbyFieldValue);
			}
		}
		GroupbyKey newKey = null;
		if(bucket.exists(key)){
			newKey = key;
		}else{
			newKey = new GroupbyKey(key);
		}
		
		bucket.addDatapoint(newKey, qualifiers);
	}

	/**
	 * @return
	 */
	public Map<List<String>, List<Double>> result(){
		return bucket.result();
	}

	public List<GroupbyKeyValue> getGroupbyKeyValues(){
		return bucket.groupbyKeyValues();
	}
}
