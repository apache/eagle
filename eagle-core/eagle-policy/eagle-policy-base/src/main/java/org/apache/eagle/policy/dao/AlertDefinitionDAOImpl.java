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
package org.apache.eagle.policy.dao;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.eagle.policy.common.Constants;
import org.apache.eagle.policy.entity.AlertDefinitionAPIEntity;
import org.apache.eagle.log.entity.GenericServiceAPIResponseEntity;
import org.apache.eagle.service.client.EagleServiceConnector;
import org.apache.eagle.service.client.IEagleServiceClient;
import org.apache.eagle.service.client.impl.EagleServiceClientImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility methods to load alert definitions for a program
 */
public class AlertDefinitionDAOImpl implements PolicyDefinitionDAO<AlertDefinitionAPIEntity> {

	private static final long serialVersionUID = 7717408104714443056L;
	private static final Logger LOG = LoggerFactory.getLogger(AlertDefinitionDAOImpl.class);
	private final EagleServiceConnector connector;

	public AlertDefinitionDAOImpl(EagleServiceConnector connector){
		this.connector = connector;
	}

    @Override
	public List<AlertDefinitionAPIEntity> findActivePolicies(String site, String dataSource) throws Exception {
		try {
			IEagleServiceClient client = new EagleServiceClientImpl(connector);
			String query = Constants.ALERT_DEFINITION_SERVICE_ENDPOINT_NAME + "[@site=\"" + site + "\" AND @dataSource=\"" + dataSource + "\"]{*}";
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
	public Map<String, Map<String, AlertDefinitionAPIEntity>> findActivePoliciesGroupbyExecutorId(String site, String dataSource) throws Exception {
		List<AlertDefinitionAPIEntity> list = findActivePolicies(site, dataSource);
		Map<String, Map<String, AlertDefinitionAPIEntity>> map = new HashMap<String, Map<String, AlertDefinitionAPIEntity>>();
			for (AlertDefinitionAPIEntity entity : list) {
				String executorID = entity.getTags().get(Constants.ALERT_EXECUTOR_ID);
				if (map.get(executorID) == null) {
					map.put(executorID, new HashMap<String, AlertDefinitionAPIEntity>());
				}
				map.get(executorID).put(entity.getTags().get("policyId"), entity);
			}
		return map;
	}
}
