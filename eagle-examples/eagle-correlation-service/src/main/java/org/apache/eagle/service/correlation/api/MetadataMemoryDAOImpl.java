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
package org.apache.eagle.service.correlation.api;

import java.util.ArrayList;
import java.util.HashMap;

public class MetadataMemoryDAOImpl<T> implements MetadataDAO<T> {
	public  HashMap<T, ArrayList<T> > findAllGroups() {
		return MetadataMemoryManager.getInstance().findAllGroups();
	}

	public ArrayList<T> findAllMetrics() {
		return MetadataMemoryManager.getInstance().findAllMetrics();
	}

	public boolean addMetric(T id) {
		return MetadataMemoryManager.getInstance().addMetric(id);
	}

	public boolean addGroup(T id, ArrayList<T> metrics) {
		return MetadataMemoryManager.getInstance().addGroup(id, metrics);
	}
	
	public boolean checkMetric(T id) {
		return MetadataMemoryManager.getInstance().checkMetric(id);
	}
	
	public boolean checkGroup(T id) {
		return MetadataMemoryManager.getInstance().checkGroup(id);
	}
}