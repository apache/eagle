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

import com.typesafe.config.Config;
import org.apache.eagle.alert.common.AlertConstants;
import org.apache.eagle.alert.entity.AlertDefinitionAPIEntity;
import org.apache.eagle.common.config.EagleConfigConstants;
import org.apache.eagle.log.entity.GenericServiceAPIResponseEntity;
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
	private final String eagleServiceHost;
	private final Integer eagleServicePort;
	private String username;
	private String password;
	private final Logger LOG = LoggerFactory.getLogger(AlertDefinitionDAOImpl.class);

	public AlertDefinitionDAOImpl(String eagleServiceHost, Integer eagleServicePort) {
		this(eagleServiceHost, eagleServicePort, null, null);
	}

	public AlertDefinitionDAOImpl(String eagleServiceHost, Integer eagleServicePort, String username, String password) {
		this.eagleServiceHost = eagleServiceHost;
		this.eagleServicePort = eagleServicePort;
		this.username = username;
		this.password = password;
	}

	public AlertDefinitionDAOImpl(Config config){
		this.eagleServiceHost = config.getString(EagleConfigConstants.EAGLE_PROPS + "." + EagleConfigConstants.EAGLE_SERVICE + "." + EagleConfigConstants.HOST);
		this.eagleServicePort = config.getInt(EagleConfigConstants.EAGLE_PROPS + "." + EagleConfigConstants.EAGLE_SERVICE + "." + EagleConfigConstants.PORT);
		if (config.hasPath(EagleConfigConstants.EAGLE_PROPS + "." + EagleConfigConstants.EAGLE_SERVICE + "." + EagleConfigConstants.USERNAME) &&
			config.hasPath(EagleConfigConstants.EAGLE_PROPS + "." + EagleConfigConstants.EAGLE_SERVICE + "." + EagleConfigConstants.PASSWORD)) {
			this.username = config.getString(EagleConfigConstants.EAGLE_PROPS + "." + EagleConfigConstants.EAGLE_SERVICE + "." + EagleConfigConstants.USERNAME);
			this.password = config.getString(EagleConfigConstants.EAGLE_PROPS + "." + EagleConfigConstants.EAGLE_SERVICE + "." + EagleConfigConstants.PASSWORD);
		}
	}

    @Override
	public List<AlertDefinitionAPIEntity> findActiveAlertDefs(String site, String dataSource) throws Exception {
		try {
			IEagleServiceClient client = new EagleServiceClientImpl(eagleServiceHost, eagleServicePort, username, password);
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
		Map<String, Map<String, AlertDefinitionAPIEntity>> map = new HashMap<String, Map<String, AlertDefinitionAPIEntity>>();
			for (AlertDefinitionAPIEntity entity : list) {
				String executorID = entity.getTags().get(AlertConstants.ALERT_EXECUTOR_ID);
				if (map.get(executorID) == null) {
					map.put(executorID, new HashMap<String, AlertDefinitionAPIEntity>());
				}
				map.get(executorID).put(entity.getTags().get("policyId"), entity);
			}
		return map;
	}
}
