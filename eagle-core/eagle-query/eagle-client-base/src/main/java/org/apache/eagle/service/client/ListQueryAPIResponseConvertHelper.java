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
package org.apache.eagle.service.client;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;

import org.apache.eagle.log.base.taggedlog.TaggedLogAPIEntity;
import org.apache.eagle.log.entity.ListQueryAPIResponseEntity;

/**
 * TODO: It's just a temporary solution. We need fix jersy and jackson mapping issue so the class
 * can be safely removed. 
 *
 */
public final class ListQueryAPIResponseConvertHelper {

	
	private static final Map<Class<?>, Map<String, Method>> BEAN_SETTER_MAP = new ConcurrentHashMap<Class<?>, Map<String, Method>>();
	private static final String SETTER_PREFIX = "set";
	
	@SuppressWarnings({ "unchecked" })
	public static ListQueryAPIResponseEntity convert(Class<? extends TaggedLogAPIEntity> clazz, ListQueryAPIResponseEntity response) throws InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException, JsonGenerationException, JsonMappingException, IOException {
		if (response == null || response.getObj() == null) {
			return response;
		}
		final List<TaggedLogAPIEntity> entities = new ArrayList<TaggedLogAPIEntity>();
		final Object obj = response.getObj();
		final Map<String, Method> settings = getOrCreateSetterMap(clazz);
		final Collection<Map<String, Object>> objs = (Collection<Map<String, Object>>) obj;
		for (Map<String, Object> map : objs) {
			final TaggedLogAPIEntity entity = clazz.newInstance();
			for (Map.Entry<String, Object> entry : map.entrySet()) {
				final String propertyName = entry.getKey();
				Object value = entry.getValue();
				final Method method = settings.get(propertyName);
				final Type type = method.getGenericParameterTypes()[0];
				if ((type == double.class || type == Double.class || type == long.class || type == Long.class)
					&& (value.equals("NaN"))) {
					value = 0;
				}					
				
				final Class<?> parameterClass = method.getParameterTypes()[0];
				if (value instanceof Number || value instanceof String || parameterClass.isInstance(value)) {
					try {
						method.invoke(entity, value);
					}
					catch (Exception e){
						e.printStackTrace();
					}
				} else {
					ObjectMapper om = new ObjectMapper();
					String objJson = om.writeValueAsString(value);
					value = om.readValue(objJson, parameterClass);
					method.invoke(entity, value);
				}
			}
			entities.add(entity);
		}
		response.setObj(entities);
		return response;
	}
	
	private static Map<String, Method> getOrCreateSetterMap(Class<?> clazz) {
		Map<String, Method> setterMap = BEAN_SETTER_MAP.get(clazz);
		if (setterMap == null) {
			setterMap = createSetterMap(clazz);
		}
		return setterMap;
	}

	private static Map<String, Method> createSetterMap(Class<?> clazz) {
		final Map<String, Method> setterMap = new HashMap<String, Method>();
		final Method[] methods = clazz.getMethods();
		final StringBuilder sb = new StringBuilder(100);
		for (Method m : methods) {
			final String methodName = m.getName();
			if (methodName.startsWith(SETTER_PREFIX) && methodName.length() > SETTER_PREFIX.length()) {
				sb.setLength(0);
				final char c = methodName.charAt(3);
				sb.append(Character.toLowerCase(c));
				sb.append(methodName.substring(4));
				String propertyName = sb.toString(); 
				setterMap.put(propertyName, m);
			}
		}
		BEAN_SETTER_MAP.put(clazz, setterMap);
		return setterMap;
	}

}
