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
package org.apache.eagle.log.entity;

import org.apache.eagle.log.base.taggedlog.TaggedLogAPIEntity;
import org.apache.eagle.log.expression.ExpressionParser;
import org.apache.eagle.log.entity.meta.*;
import org.apache.eagle.query.parser.TokenConstant;
import org.apache.eagle.common.ByteUtil;
import org.apache.eagle.common.EagleBase64Wrapper;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class HBaseInternalLogHelper {
	private final static Logger LOG  = LoggerFactory.getLogger(HBaseInternalLogHelper.class);

	private static final EntitySerDeserializer ENTITY_SERDESER = new EntitySerDeserializer();

	/**
	 *
	 * @param ed
	 * @param r
	 * @param qualifiers if null, return all qualifiers defined in ed
	 * @return
	 */
	public static InternalLog parse(EntityDefinition ed, Result r, byte[][] qualifiers) {
		final byte[] row = r.getRow();
		// skip the first 4 bytes : prefix
		final int offset = (ed.getPartitions() == null) ? (4) : (4 + ed.getPartitions().length * 4);
		long timestamp = ByteUtil.bytesToLong(row, offset);
		// reverse timestamp
		timestamp = Long.MAX_VALUE - timestamp;
		final byte[] family = ed.getColumnFamily().getBytes();
		final Map<String, byte[]> allQualifierValues = new HashMap<String, byte[]>();

		if (qualifiers != null) {
			int count = qualifiers.length;
			final byte[][] values = new byte[count][];
			for (int i = 0; i < count; i++) {
				// TODO if returned value is null, it means no this column for this row, so why set null to the object?
				values[i] = r.getValue(family, qualifiers[i]);
				allQualifierValues.put(new String(qualifiers[i]), values[i]);
			}
		}else{
			// return all qualifiers
			for(KeyValue kv:r.list()){
				byte[] qualifier = kv.getQualifier();
				byte[] value = kv.getValue();
				allQualifierValues.put(new String(qualifier),value);
			}
		}
		final InternalLog log = buildObject(ed, row, timestamp, allQualifierValues);
		return log;
	}

	/**
	 *
	 * @param ed
	 * @param row
	 * @param timestamp
	 * @param allQualifierValues <code>Map &lt; Qualifier name (not display name),Value in bytes array &gt;</code>
	 * @return
	 */
	public static InternalLog buildObject(EntityDefinition ed, byte[] row, long timestamp, Map<String, byte[]> allQualifierValues) {
		InternalLog log = new InternalLog();
		String myRow = EagleBase64Wrapper.encodeByteArray2URLSafeString(row);
		log.setEncodedRowkey(myRow);
		log.setPrefix(ed.getPrefix());
		log.setTimestamp(timestamp);

		Map<String, byte[]> logQualifierValues = new HashMap<String, byte[]>();
		Map<String, String> logTags = new HashMap<String, String>();
		Map<String, Object> extra = null;

		Map<String,Double> doubleMap = null;
		// handle with metric
		boolean isMetricEntity = GenericMetricEntity.GENERIC_METRIC_SERVICE.equals(ed.getService());
		double[] metricValueArray = null;

		for (Map.Entry<String, byte[]> entry : allQualifierValues.entrySet()) {
			if (ed.isTag(entry.getKey())) {
				if (entry.getValue() != null) {
					logTags.put(entry.getKey(), new String(entry.getValue()));
				}else if (TokenConstant.isExpression(entry.getKey())){
					if(doubleMap == null) doubleMap = EntityQualifierUtils.bytesMapToDoubleMap(allQualifierValues, ed);
					// Caculate expression based fields
					String expression = TokenConstant.parseExpressionContent(entry.getKey());
					if (extra == null) extra = new HashMap<String, Object>();

					// Evaluation expression as output based on entity
					// -----------------------------------------------
					// 1) Firstly, check whether is metric entity and expression requires value and also value is not number (i.e. double[])
					// 2) Treat all required fields as double, if not number, then set result as NaN

					try {
						ExpressionParser parser = ExpressionParser.parse(expression);
						boolean isRequiringValue = parser.getDependentFields().contains(GenericMetricEntity.VALUE_FIELD);

						if(isMetricEntity && isRequiringValue && doubleMap.get(GenericMetricEntity.VALUE_FIELD)!=null
								&& Double.isNaN(doubleMap.get(GenericMetricEntity.VALUE_FIELD))) // EntityQualifierUtils will convert non-number field into Double.NaN
						{
							// if dependent fields require "value"
							// and value exists but value's type is double[] instead of double

							// handle with metric value array based expression
							// lazily extract metric value as double array if required
							if(metricValueArray == null){
								// if(allQualifierValues.containsKey(GenericMetricEntity.VALUE_FIELD)){
								Qualifier qualifier = ed.getDisplayNameMap().get(GenericMetricEntity.VALUE_FIELD);
								EntitySerDeser serDeser = qualifier.getSerDeser();
								if(serDeser instanceof DoubleArraySerDeser){
									byte[] value = allQualifierValues.get(qualifier.getQualifierName());
									if(value !=null ) metricValueArray = (double[]) serDeser.deserialize(value);
								}
								// }
							}

							if(metricValueArray!=null){
								double[] resultBucket = new double[metricValueArray.length];
								Map<String, Double> _doubleMap = new HashMap<String,Double>(doubleMap);
								_doubleMap.remove(entry.getKey());
								for(int i=0;i< resultBucket.length;i++) {
									_doubleMap.put(GenericMetricEntity.VALUE_FIELD, metricValueArray[i]);
									resultBucket[i]=  parser.eval(_doubleMap);
								}
								extra.put(expression,resultBucket);
							}else{
								LOG.warn("Failed convert metric value into double[] type which is required by expression: "+expression);
								// if require value in double[] is NaN
								double value = parser.eval(doubleMap);
								extra.put(expression, value);
							}
						}else {
							double value = parser.eval(doubleMap);
							extra.put(expression, value);
							// LOG.info("DEBUG: "+entry.getKey()+" = "+ value);
						}
					} catch (Exception e) {
						LOG.error("Failed to eval expression "+expression+", exception: "+e.getMessage(),e);
					}
				}
			} else {
				logQualifierValues.put(entry.getKey(),entry.getValue());
			}
		}
		log.setQualifierValues(logQualifierValues);
		log.setTags(logTags);
		log.setExtraValues(extra);
		return log;
	}
	
	public static TaggedLogAPIEntity buildEntity(InternalLog log, EntityDefinition entityDef) throws Exception {
		Map<String, byte[]> qualifierValues = log.getQualifierValues();
		TaggedLogAPIEntity entity = ENTITY_SERDESER.readValue(qualifierValues, entityDef);
		if (entity.getTags() == null && log.getTags() != null) {
			entity.setTags(log.getTags());
		}
		entity.setExp(log.getExtraValues());
		entity.setTimestamp(log.getTimestamp());
		entity.setEncodedRowkey(log.getEncodedRowkey());
		entity.setPrefix(log.getPrefix());
		return entity;
	}
	
	public static List<TaggedLogAPIEntity> buildEntities(List<InternalLog> logs, EntityDefinition entityDef) throws Exception {
		final List<TaggedLogAPIEntity> result = new ArrayList<TaggedLogAPIEntity>(logs.size());
		for (InternalLog log : logs) {
			result.add(buildEntity(log, entityDef));
		}
		return result;
	}
	
	public static byte[][] getOutputQualifiers(EntityDefinition entityDef, List<String> outputFields) {
		final byte[][] result = new byte[outputFields.size()][];
		int index = 0;
		for(String field : outputFields){
			// convert displayName to qualifierName
			Qualifier q = entityDef.getDisplayNameMap().get(field);
			if(q == null){ // for tag case
				result[index++] = field.getBytes();
			}else{ // for qualifier case
				result[index++] = q.getQualifierName().getBytes();
			}
		}
		return result;
	}

	public static InternalLog convertToInternalLog(TaggedLogAPIEntity entity, EntityDefinition entityDef) throws Exception {
		final InternalLog log = new InternalLog();
		final Map<String, String> inputTags = entity.getTags();
		final Map<String, String> tags = new TreeMap<String, String>();
		if(inputTags!=null) {
			for (Map.Entry<String, String> entry : inputTags.entrySet()) {
				tags.put(entry.getKey(), entry.getValue());
			}
		}
		log.setTags(tags);
		if(entityDef.isTimeSeries()){
			log.setTimestamp(entity.getTimestamp());
		}else{
			log.setTimestamp(EntityConstants.FIXED_WRITE_TIMESTAMP); // set timestamp to MAX, then actually stored 0
		}
		
		// For Metric entity, prefix is populated along with entity instead of EntityDefinition
		if(entity.getPrefix() != null && !entity.getPrefix().isEmpty()){
			log.setPrefix(entity.getPrefix());
		}else{
			log.setPrefix(entityDef.getPrefix());
		}
		
		log.setPartitions(entityDef.getPartitions());
		EntitySerDeserializer des = new EntitySerDeserializer();
		log.setQualifierValues(des.writeValue(entity, entityDef));
		
		final IndexDefinition[] indexDefs = entityDef.getIndexes();
		if (indexDefs != null) {
			final List<byte[]> indexRowkeys = new ArrayList<byte[]>();
			for (int i = 0; i < indexDefs.length; ++i) {
				final IndexDefinition indexDef = indexDefs[i];
				final byte[] indexRowkey = indexDef.generateIndexRowkey(entity);
				indexRowkeys.add(indexRowkey);
			}
			log.setIndexRowkeys(indexRowkeys);
		}
		return log;
	}
}
