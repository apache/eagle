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
package org.apache.eagle.common.metric;

import org.apache.log4j.spi.LoggerFactory;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * not thread safe
 */
public class AlertContext implements Serializable{
	private static final Logger LOG = org.slf4j.LoggerFactory.getLogger(AlertContext.class);

	private Map<String, String> properties = new HashMap<String, String>();
	
	public AlertContext(){
	}
	
	public AlertContext(AlertContext context){
		this.properties = new HashMap<String, String>(context.properties);
	}
	
	public String removeProperty(String name)
	{
		return properties.remove(name);
	}
	
	public AlertContext addProperty(String name, String value){
		properties.put(name, value);
		return this;
	}

	public AlertContext addAll(Map<String,String> propHash){
		this.properties.putAll(propHash);
		return this;
	}

	public String toJsonString(){
		ObjectMapper objectMapper = new ObjectMapper();
		try {
			return objectMapper.writeValueAsString(properties);
		}catch(Exception ex){
			LOG.error("fail converting alertcontext into string", ex);
			return null;
		}
	}

	public static AlertContext fromJsonString(String json){
		ObjectMapper objectMapper = new ObjectMapper();
		try{
			Map m = objectMapper.readValue(json, Map.class);
			AlertContext c = new AlertContext();
			c.addAll(m);
			return c;
		}catch(Exception ex){
			LOG.error("fail converting string into alertcontext", ex);
			return null;
		}
	}

	public String getProperty(String name){
		return properties.get(name);
	}
	
	public String toString(){
		return properties.toString();
	}

	public Map<String, String> getProperties(){
		return properties;
	}
	
	public void setProperties(Map<String, String> properties){
		this.properties = properties;
	}
}
