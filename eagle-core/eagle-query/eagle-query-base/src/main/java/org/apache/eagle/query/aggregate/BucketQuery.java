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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.eagle.log.base.taggedlog.TaggedLogAPIEntity;

public class BucketQuery {
	public final static String UNASSIGNED_BUCKET = "unassigned"; 
	private List<String> bucketFields;
	private int limit;
	private Map<String, Object> root = new HashMap<String, Object>();
	
	public BucketQuery(List<String> bucketFields, int limit){
		this.bucketFields = bucketFields;
		this.limit = limit;
	}
	
	@SuppressWarnings("unchecked")
	public void put(TaggedLogAPIEntity entity){
		Map<String, Object> current = root;
		int bucketCount = bucketFields.size();
		if(bucketCount <= 0)
			return; // silently return
		int i = 0;
		String bucketFieldValue = null;
		for(; i<bucketCount; i++){
			String bucketField = bucketFields.get(i);
			bucketFieldValue = entity.getTags().get(bucketField);
			if(bucketFieldValue == null || bucketFieldValue.isEmpty()){
				bucketFieldValue = UNASSIGNED_BUCKET;
			}
			// for last bucket, bypass the following logic
			if(i == bucketCount-1){
				break;
			}
				
			if(current.get(bucketFieldValue) == null){
				current.put(bucketFieldValue, new HashMap<String, Object>());
			}
			// for the last level of bucket, it is not Map, instead it is List<TaggedLogAPIEntity> 
			current = (Map<String, Object>)current.get(bucketFieldValue);
		}
		List<TaggedLogAPIEntity> bucketContent = (List<TaggedLogAPIEntity>)current.get(bucketFieldValue);
		if(bucketContent == null){
			bucketContent = new ArrayList<TaggedLogAPIEntity>();
			current.put(bucketFieldValue, bucketContent);
		}
		
		if(bucketContent.size() >= limit){
			return;
		}else{
			bucketContent.add(entity);
		}
	}
	
	public void batchPut(List<TaggedLogAPIEntity> entities){
		for(TaggedLogAPIEntity entity : entities){
			put(entity);
		}
	}
	
	public Map<String, Object> get(){
		return root;
	}
}
