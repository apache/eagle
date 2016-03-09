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
package org.apache.eagle.dataproc.core;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.ObjectMapper;

public class JsonSerDeserUtils {
	private static final Logger LOG = LoggerFactory.getLogger(JsonSerDeserUtils.class);

	public static <T> String serialize(T o) throws Exception{
		return serialize(o, null);
	}
	
	public static <T> String serialize(T o, List<Module> modules) throws Exception {
		ObjectMapper mapper = new ObjectMapper();
		if (modules != null) { 
			mapper.registerModules(modules);
		}
		return mapper.writeValueAsString(o);
	}

	public static <T> T deserialize(String value, Class<T> cls) throws Exception{
		return deserialize(value, cls, null);
	}
	
	public static <T> T deserialize(String value, Class<T> cls, List<Module> modules) throws Exception{
		ObjectMapper mapper = new ObjectMapper();
		if (modules != null) { 
			mapper.registerModules(modules);
		}
		return mapper.readValue(value, cls);	
	}
}
