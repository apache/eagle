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

import org.apache.eagle.alert.common.AlertConstants;
import org.apache.eagle.alert.entity.AlertDefinitionAPIEntity;
import org.apache.eagle.log.entity.GenericServiceAPIResponseEntity;
import org.apache.eagle.service.client.EagleServiceConnector;
import org.apache.eagle.service.client.IEagleServiceClient;
import org.apache.eagle.service.client.impl.EagleServiceClientImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Utility methods to load alert definitions for a program
 */
public class AlertDefinitionDAOImpl implements AlertDefinitionDAO {
	private final Logger LOG = LoggerFactory.getLogger(AlertDefinitionDAOImpl.class);
	private final EagleServiceConnector connector;

	public AlertDefinitionDAOImpl(EagleServiceConnector connector){
		this.connector = connector;
	}

    @Override
	public List<AlertDefinitionAPIEntity> findActiveAlertDefs(String site, String dataSource) throws Exception {
		try {
			IEagleServiceClient client = new EagleServiceClientImpl(connector);
			String query = AlertConstants.ALERT_DEFINITION_SERVICE_ENDPOINT_NAME + "[@site=\"" + site + "\" AND @dataSource=\"" + dataSource + "\"]{*}";
			GenericServiceAPIResponseEntity<AlertDefinitionAPIEntity> response =  client.search()
																		                .pageSize(Integer.MAX_VALUE)
																		                .query(query)
																	                    .send();
			client.close();
			if (response.getException() != null) {
				throw new Exception("Got an exception when query eagle service: " + response.getException()); 
			}
			List<AlertDefinitionAPIEntity> list = response.getObj();
			List<AlertDefinitionAPIEntity> enabledList = new ArrayList<AlertDefinitionAPIEntity>();
			for (AlertDefinitionAPIEntity entity : list) {
				if (entity.isEnabled()) enabledList.add(entity);
			}
			return enabledList;
		}
		catch (Exception ex) {
			LOG.error("Got an exception when query alert Def service", ex);
			throw new IllegalStateException(ex);
		}					   
	}

    @Override
	public Map<String, Map<String, AlertDefinitionAPIEntity>> findActiveAlertDefsGroupbyAlertExecutorId(String site, String dataSource) throws Exception {
		List<AlertDefinitionAPIEntity> list = findActiveAlertDefs(site, dataSource);
		return groupByAlertExecutorId(list);
	}

	@Override
	public  Map<String, AlertDefinitionAPIEntity> findActiveAlertDefsByNotification( String site, String dataSource , String notificationType ) throws Exception {
		List<AlertDefinitionAPIEntity> enabledList = new ArrayList<AlertDefinitionAPIEntity>();
		try {
			IEagleServiceClient client = new EagleServiceClientImpl(connector);
			String query = AlertConstants.ALERT_DEFINITION_SERVICE_ENDPOINT_NAME + "[@site=\"" + site + "\" AND @dataSource=\"" + dataSource + "\" AND @notificationType=\"" + notificationType + "\"]{*}";
			GenericServiceAPIResponseEntity<AlertDefinitionAPIEntity> response =  client.search()
					.pageSize(Integer.MAX_VALUE)
					.query(query)
					.send();
			client.close();
			if (response.getException() != null) {
				throw new Exception("Got an exception when query eagle service: " + response.getException());
			}
			List<AlertDefinitionAPIEntity> list = response.getObj();
			for (AlertDefinitionAPIEntity entity : list) {
				if (entity.isEnabled()) enabledList.add(entity);
			}
		}
		catch (Exception ex) {
			LOG.error("Got an exception when query alert Def service", ex);
			throw new IllegalStateException(ex);
		}
		return groupByPolicyId(enabledList);
		}

	/**
	 * Group Active Alert List by Alert Executor ID
	 * @param list
	 * @return activeAlertsByExecutorId
     */
		public Map<String, Map<String, AlertDefinitionAPIEntity>>  groupByAlertExecutorId(List<AlertDefinitionAPIEntity> list)
		{
			Map<String, Map<String, AlertDefinitionAPIEntity>> activeAlertsMap  = new HashMap<String, Map<String, AlertDefinitionAPIEntity>>();
			for (AlertDefinitionAPIEntity entity : list) {
				String executorID = entity.getTags().get(AlertConstants.ALERT_EXECUTOR_ID);
				if (activeAlertsMap.get(executorID) == null) {
					activeAlertsMap.put(executorID, new HashMap<String, AlertDefinitionAPIEntity>());
				}
				activeAlertsMap.get(executorID).put(entity.getTags().get("policyId"), entity);
			}
			return activeAlertsMap;
		}


	/**
	 * Group Active Alert List by Alert Executor ID
	 * @param list
	 * @return activeAlertsByExecutorId
	 */
	public Map<String, AlertDefinitionAPIEntity>   groupByPolicyId ( List<AlertDefinitionAPIEntity> list )
	{
		Map<String, AlertDefinitionAPIEntity> activeAlertsMap  = new HashMap<String , AlertDefinitionAPIEntity>();
		for (AlertDefinitionAPIEntity entity : list) {
			String policyId = entity.getTags().get(AlertConstants.POLICY_ID);
			if (activeAlertsMap.get(policyId) == null) {
				activeAlertsMap.put(policyId, entity);
			}
		}
		return activeAlertsMap;
	}

}
