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
package org.apache.eagle.log.entity.meta;

import org.apache.eagle.log.base.taggedlog.TaggedLogAPIEntity;
import org.apache.commons.beanutils.PropertyUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.beans.PropertyDescriptor;
import java.util.HashMap;
import java.util.Map;

public class EntitySerDeserializer {
	private static final Logger LOG = LoggerFactory.getLogger(EntitySerDeserializer.class);
	
	// TODO throws seperate exceptions
	@SuppressWarnings("unchecked")
	public <T> T readValue(Map<String, byte[]> qualifierValues, EntityDefinition ed) throws Exception{
		Class<? extends TaggedLogAPIEntity> clazz = ed.getEntityClass();
		if(clazz == null){
			throw new NullPointerException("Entity class of service "+ed.getService()+" is null");
		}
		TaggedLogAPIEntity obj = clazz.newInstance();
		Map<String, Qualifier> map = ed.getQualifierNameMap();
		for(Map.Entry<String, byte[]> entry : qualifierValues.entrySet()){
			Qualifier q = map.get(entry.getKey());
			if(q == null){
				// if it's not pre-defined qualifier, it must be tag unless it's a bug
				if(obj.getTags() == null){
					obj.setTags(new HashMap<String, String>());
				}
				obj.getTags().put(entry.getKey(), new StringSerDeser().deserialize(entry.getValue()));
				continue;
			}
			
			// TODO performance loss compared with new operator
			// parse different types of qualifiers
			String fieldName = q.getDisplayName();
			PropertyDescriptor pd = PropertyUtils.getPropertyDescriptor(obj, fieldName);
			if(entry.getValue() != null){
				Object args = q.getSerDeser().deserialize(entry.getValue());
				pd.getWriteMethod().invoke(obj, args);
//				if (logger.isDebugEnabled()) {
//					logger.debug(entry.getKey() + ":" + args + " is deserialized");
//				}
			}
		}
		return (T)obj;
	}
	
	public Map<String, byte[]> writeValue(TaggedLogAPIEntity entity, EntityDefinition ed) throws Exception{
		Map<String, byte[]> qualifierValues = new HashMap<String, byte[]>();
		// iterate all modified qualifiers
		for(String fieldName : entity.modifiedQualifiers()){
			PropertyDescriptor pd = PropertyUtils.getPropertyDescriptor(entity, fieldName);
			Object obj = pd.getReadMethod().invoke(entity);
			Qualifier q = ed.getDisplayNameMap().get(fieldName);
			EntitySerDeser<Object> ser = q.getSerDeser();
			byte[] value = ser.serialize(obj);
			qualifierValues.put(q.getQualifierName(), value);
		}
		return qualifierValues;
	}
}
