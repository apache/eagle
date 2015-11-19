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
import org.apache.eagle.log.entity.EntityCreationListener;
import org.apache.eagle.log.expression.ExpressionParser;
import org.apache.eagle.query.aggregate.AggregateFunctionType;
import org.apache.eagle.query.aggregate.IllegalAggregateFieldTypeException;
import org.apache.eagle.query.parser.TokenConstant;
import org.apache.commons.beanutils.PropertyUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.beans.PropertyDescriptor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

public abstract class AbstractAggregator implements Aggregator, EntityCreationListener{
	private final static Logger LOG = LoggerFactory.getLogger(AbstractAggregator.class);

	private static final String UNASSIGNED = "unassigned";
	protected List<String> groupbyFields;
	protected List<AggregateFunctionType> aggregateFunctionTypes;
	protected List<String> aggregatedFields;
	// a cache to know immediately if groupby field should come from tags(true) or qualifiers(false)
	private Boolean[] _groupbyFieldPlacementCache;
	private Method[] _aggregateFieldReflectedMethodCache;

	public AbstractAggregator(List<String> groupbyFields, List<AggregateFunctionType> aggregateFuntionTypes, List<String> aggregatedFields){
		this.groupbyFields = groupbyFields;
		this.aggregateFunctionTypes = aggregateFuntionTypes;
		this.aggregatedFields = aggregatedFields;
		_aggregateFieldReflectedMethodCache = new Method[this.aggregatedFields.size()];
		_groupbyFieldPlacementCache = new Boolean[this.groupbyFields.size()];
	}
	
	@Override
	public void entityCreated(TaggedLogAPIEntity entity) throws Exception{
		accumulate(entity);
	}
	
	public abstract Object result();
	
	protected String createGroupFromTags(TaggedLogAPIEntity entity, String groupbyField, int i){
		String groupbyFieldValue = entity.getTags().get(groupbyField);
		if(groupbyFieldValue != null){
			_groupbyFieldPlacementCache[i] = true;
			return groupbyFieldValue;
		}
		return null;
	}
	
	protected String createGroupFromQualifiers(TaggedLogAPIEntity entity, String groupbyField, int i){
		try{
			PropertyDescriptor pd = PropertyUtils.getPropertyDescriptor(entity, groupbyField);
			if(pd == null)
				return null;
//			_groupbyFieldPlacementCache.put(groupbyField, false);
			_groupbyFieldPlacementCache[i] = false;
			return (String)(pd.getReadMethod().invoke(entity));
		}catch(NoSuchMethodException ex){
			return null;
		}catch(InvocationTargetException ex){
			return null;
		}catch(IllegalAccessException ex){
			return null;
		}
	}
	
	protected String determineGroupbyFieldValue(TaggedLogAPIEntity entity, String groupbyField, int i){
		Boolean placement = _groupbyFieldPlacementCache[i];
		String groupbyFieldValue = null; 
		if(placement != null){
			groupbyFieldValue = placement.booleanValue() ? createGroupFromTags(entity, groupbyField, i) : createGroupFromQualifiers(entity, groupbyField, i); 
		}else{
			groupbyFieldValue = createGroupFromTags(entity, groupbyField, i);
			if(groupbyFieldValue == null){
				groupbyFieldValue = createGroupFromQualifiers(entity, groupbyField, i);
			}
		}
		groupbyFieldValue = (groupbyFieldValue == null ? UNASSIGNED : groupbyFieldValue);
		return groupbyFieldValue;
	}
	
	/**
	 * TODO For count aggregation, special treatment is the value is always 0 unless we support count(*) or count(<fieldname>) which counts number of rows or 
	 * number of non-null field
	 * For other aggregation, like sum,min,max,avg, we should resort to qualifiers
	 * @param entity
	 * @return
	 */
	protected List<Double> createPreAggregatedValues(TaggedLogAPIEntity entity) throws Exception{
		List<Double> values = new ArrayList<Double>();
		int functionIndex = 0;
		for(AggregateFunctionType type : aggregateFunctionTypes){
			if(type.name().equals(AggregateFunctionType.count.name())){
				values.add(new Double(1));
			}else{
				// find value in qualifier by checking java bean
				String aggregatedField = aggregatedFields.get(functionIndex);
				if(TokenConstant.isExpression(aggregatedField)){
					try {
						String expr = TokenConstant.parseExpressionContent(aggregatedField);
						values.add(ExpressionParser.eval(expr, entity));
					}catch (Exception ex){
						LOG.error("Failed to evaluate expression-based aggregation: " + aggregatedField, ex);
						throw ex;
					}
				}else {
					try {
						Method m = _aggregateFieldReflectedMethodCache[functionIndex];
						if (m == null) {
//						pd = PropertyUtils.getPropertyDescriptor(entity, aggregatedField);
//						if (pd == null) {
//							final String errMsg = "Field/tag " + aggregatedField + " is not defined for entity " + entity.getClass().getSimpleName();
//							logger.error(errMsg);
//							throw new Exception(errMsg);
//						}
//						Object obj = pd.getReadMethod().invoke(entity);
							String tmp = aggregatedField.substring(0, 1).toUpperCase() + aggregatedField.substring(1);
							m = entity.getClass().getMethod("get" + tmp);
							_aggregateFieldReflectedMethodCache[functionIndex] = m;
						}
						Object obj = m.invoke(entity);
						values.add(numberToDouble(obj));
					} catch (Exception ex) {
						LOG.error("Cannot do aggregation for field " + aggregatedField, ex);
						throw ex;
					}
				}
			}
			functionIndex++;
		}
		return values;
	}
	
	/**
	 * TODO this is a hack, we need elegant way to convert type to a broad precision
     *
	 * @param obj
	 * @return
	 */
	protected Double numberToDouble(Object obj){
		if(obj instanceof Double)
			return (Double)obj;
		if(obj instanceof Integer){
			return new Double(((Integer)obj).doubleValue());
		}
		if(obj instanceof Long){
			return new Double(((Long)obj).doubleValue());
		}
		// TODO hack to support string field for demo purpose, should be removed
		if(obj == null){
			return new Double(0.0);
		}
		if(obj instanceof String){
			try{
				return new Double((String)obj);
			}catch(Exception ex){
				LOG.warn("Datapoint ignored because it can not be converted to correct number for " + obj, ex);
				return new Double(0.0);
			}
		}
		
		throw new IllegalAggregateFieldTypeException(obj.getClass().toString() + " type is not support. The aggregated field must be numeric type, int, long or double");
	}
}
