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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.eagle.alert.common.AlertConstants;
import org.apache.eagle.alert.entity.AbstractPolicyDefinitionEntity;
import org.apache.eagle.log.entity.GenericServiceAPIResponseEntity;
import org.apache.eagle.service.client.EagleServiceConnector;
import org.apache.eagle.service.client.IEagleServiceClient;
import org.apache.eagle.service.client.impl.EagleServiceClientImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @since Dec 17, 2015
 *
 */
public class PolicyEnityDAOImpl<T extends AbstractPolicyDefinitionEntity> implements PolicyDefinitionDAO<T> {

	private static final long serialVersionUID = 1L;

	private static final Logger LOG = LoggerFactory.getLogger(PolicyEnityDAOImpl.class);
	private final EagleServiceConnector connector;
	private final String servicePointName;

	public PolicyEnityDAOImpl(EagleServiceConnector connector, String serviceName){
		this.connector = connector;
		this.servicePointName = serviceName;
	}

    @Override
	public List<T> findActivePolicies(String site, String dataSource) throws Exception {
		try {
			IEagleServiceClient client = new EagleServiceClientImpl(connector);
			String query = servicePointName + "[@site=\"" + site + "\" AND @dataSource=\"" + dataSource + "\"]{*}";
			GenericServiceAPIResponseEntity<T> response = client.search()
												                .pageSize(Integer.MAX_VALUE)
												                .query(query)
											                    .send();
			client.close();
			if (response.getException() != null) {
				throw new Exception("Got an exception when query eagle service: " + response.getException()); 
			}
			List<T> list = response.getObj();
			List<T> enabledList = new ArrayList<T>();
			for (T entity : list) {
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
	public Map<String, Map<String, T>> findActivePoliciesGroupbyExecutorId(String site, String dataSource)
			throws Exception {
		List<T> list = findActivePolicies(site, dataSource);
		Map<String, Map<String, T>> map = new HashMap<String, Map<String, T>>();
		for (T entity : list) {
			// support both executorId and legacy alertExecutorId
			String executorID = entity.getTags().containsKey("executorId") ? entity.getTags().get("executorId")
					: entity.getTags().get(AlertConstants.ALERT_EXECUTOR_ID);

			if (map.get(executorID) == null) {
				map.put(executorID, new HashMap<String, T>());
			}
			map.get(executorID).put(entity.getTags().get("policyId"), entity);
		}
		return map;
	}

}
