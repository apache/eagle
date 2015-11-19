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
package org.apache.eagle.security.util;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.quartz.Job;

public class ExternalDataCache {
	private Map<Class<? extends Job>, Object> map = new ConcurrentHashMap<Class<? extends Job>, Object>();
	
	private static final ExternalDataCache instance = new ExternalDataCache();
	
	private ExternalDataCache(){
	}
	
	public static ExternalDataCache getInstance(){
		return instance;
	}
	
	public Object getJobResult(Class<? extends Job> cls){
		return map.get(cls);
	}
	
	public void setJobResult(Class<? extends Job> cls, Object jobResult){
		map.put(cls, jobResult);
	}
}
