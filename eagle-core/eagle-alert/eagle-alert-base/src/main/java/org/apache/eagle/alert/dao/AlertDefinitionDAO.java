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
package org.apache.eagle.alert.dao;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

import org.apache.eagle.alert.entity.AlertDefinitionAPIEntity;

public interface AlertDefinitionDAO extends Serializable{
	/**
	 * find list of active alert definitions for one specific site and dataSource
	 * @return
	 */
	List<AlertDefinitionAPIEntity> findActiveAlertDefs(String site, String dataSource) throws Exception;
	
	/**
	 * find map from alertExecutorId to map from policy Id to alert definition for one specific site and dataSource
	 * Map from alertExecutorId to map from policyId to policy definition
       (site,dataSource) => Map[alertExecutorId,Map[policyId,alertDefinition]]
	 * @return
	 */
	Map<String, Map<String, AlertDefinitionAPIEntity>> findActiveAlertDefsGroupbyAlertExecutorId(String site, String dataSource) throws Exception;
}
