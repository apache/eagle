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

import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.eagle.log.base.taggedlog.TaggedLogAPIEntity;

public class Aggregator {
	private static final Logger LOG = LoggerFactory.getLogger(Aggregator.class);
	public static final String GROUPBY_ROOT_FIELD_NAME = "site";
	public static final String GROUPBY_ROOT_FIELD_VALUE = "xyz";
	public static final String UNASSIGNED_GROUPBY_ROOT_FIELD_NAME = "unassigned";
	
	private final AggregateAPIEntityFactory factory;
	private final AggregateAPIEntity root;
	private final List<String> groupbys;
	private final List<String> sumFunctionFields;
	private final boolean counting;
	
	public Aggregator(AggregateAPIEntityFactory factory, AggregateAPIEntity root, List<String> groupbys, boolean counting, List<String> sumFunctionFields){
		this.factory = factory;
		this.root = root;
		this.groupbys = groupbys;
		this.sumFunctionFields = sumFunctionFields;
		this.counting = counting;
	}

	/**
	 * this locate result can be cached? we don't need check if it's TaggedLogAPIEntity each time when iterating entities
	 * @param groupby
	 * @param obj
	 * @return
	 * @throws Exception
	 */
	private String locateGroupbyField(String groupby, TaggedLogAPIEntity obj){
		if(groupby.equals(GROUPBY_ROOT_FIELD_NAME)){
			return GROUPBY_ROOT_FIELD_VALUE;
		}
		// check tag first
		String tagv = obj.getTags().get(groupby);
		if(tagv != null)
			return tagv;
		// check against pojo, or qualifierValues
		String fn = groupby.substring(0,1).toUpperCase()+groupby.substring(1, groupby.length());
		try{
			Method getM = obj.getClass().getMethod("get"+fn);
			Object value = getM.invoke(obj);
			return (String)value;
		}catch(Exception ex){
			LOG.warn(groupby + " field is in neither tags nor fields, " + ex.getMessage());
			return null;
		}
	}
	
	/**
	 * accumulate a list of entities
	 * @param entities
	 * @throws Exception
	 */
	public void accumulateAll(List<TaggedLogAPIEntity> entities) throws Exception{
		for(TaggedLogAPIEntity entity : entities){
			accumulate(entity);
		}
	}
	
	/**
	 * currently only group by tags
	 * groupbys' first item always is site, which is a reserved field 
	 */
	public void accumulate(TaggedLogAPIEntity entity) throws Exception{
		AggregateAPIEntity current = root;
		for(String groupby : groupbys){
			// TODO tagv is empty, so what to do? use a reserved field_name "unassigned" ?
			// TODO we should support all Pojo with java bean style object
			String tagv = locateGroupbyField(groupby, entity);
			if(tagv == null || tagv.isEmpty()){
				tagv = UNASSIGNED_GROUPBY_ROOT_FIELD_NAME;
			}
			Map<String, AggregateAPIEntity> children = current.getEntityList();
			if(children.get(tagv) == null){
				children.put(tagv, factory.create());
				current.setNumDirectDescendants(current.getNumDirectDescendants()+1);
			}
			AggregateAPIEntity child = children.get(tagv);
			// go through all aggregate functions including count, summary etc.			
			if(counting)
				count(child);
			for(String sumFunctionField : sumFunctionFields){
				sum(child, entity, sumFunctionField);
			}
			
			current = child;
		}
		
	}

	
	/**
	 * use java bean specifications?
	 * reflection is not efficient, let us find out solutions
	 */
	private void sum(Object targetObj, TaggedLogAPIEntity srcObj, String fieldName) throws Exception{
		try{
			String fn = fieldName.substring(0,1).toUpperCase()+fieldName.substring(1, fieldName.length());
			Method srcGetMethod = srcObj.getClass().getMethod("get"+fn);
			Object srcValue = srcGetMethod.invoke(srcObj);
			if(srcValue == null){
				return;  // silently don't count this source object
			}
			Method targetGetMethod = targetObj.getClass().getMethod("get"+fn);
			Object targetValue = targetGetMethod.invoke(targetObj);
			if(targetValue instanceof Long){
				Method setM = targetObj.getClass().getMethod("set"+fn, long.class);
				Long tmp1 = (Long)targetValue;
				// TODO, now source object always have type "java.lang.String", later on we should support various type including integer type
				Long tmp2 = null;
				if(srcValue instanceof String){
					tmp2 = Long.valueOf((String)srcValue);
				}else if(srcValue instanceof Long){
					tmp2 = (Long)srcValue;
				}else{
					throw new IllegalAggregateFieldTypeException(srcValue.getClass().toString() + " type is not support. The source type must be Long or String");
				}
				setM.invoke(targetObj, tmp1.longValue()+tmp2.longValue());
			}else if(targetValue instanceof Double){
				Method setM = targetObj.getClass().getMethod("set"+fn, double.class);
				Double tmp1 = (Double)targetValue;
				String src = (String) srcValue;
				Double tmp2 = Double.valueOf(src);
				setM.invoke(targetObj, tmp1.doubleValue()+tmp2.doubleValue());
			}else{
				throw new IllegalAggregateFieldTypeException(targetValue.getClass().toString() + " type is not support. The target type must be long or double");
			}
		}catch(Exception ex){
			LOG.error("Cannot do sum aggregation for field " + fieldName, ex);
			throw ex;
		}
	}
	
	/**
	 * count possible not only count for number of descendants but also count for not-null fields 
	 * @param targetObj
	 * @throws Exception
	 */
	private void count(AggregateAPIEntity targetObj) throws Exception{
		targetObj.setNumTotalDescendants(targetObj.getNumTotalDescendants()+1);
	}
}