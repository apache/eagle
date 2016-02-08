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

package org.apache.eagle.notification.utils;


import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.type.CollectionType;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigObject;
import org.apache.eagle.alert.entity.AlertDefinitionAPIEntity;
import org.apache.eagle.common.config.EagleConfigFactory;
import org.apache.eagle.notification.NotificationManager;
import org.apache.eagle.policy.common.Constants;
import org.apache.eagle.policy.dao.PolicyDefinitionDAO;
import org.apache.eagle.policy.dao.PolicyDefinitionEntityDAOImpl;
import org.apache.eagle.service.client.EagleServiceConnector;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Common methods for Notification Plugin
 */
public class NotificationPluginUtils {

	private static ConfigObject _conf;
	private static Config   _config;
	private static final ObjectMapper mapper = new ObjectMapper();
	private static final CollectionType mapCollectionType = mapper.getTypeFactory().constructCollectionType(List.class, Map.class);


	/**
	 * Initially Client should set the Config Object
	 * @param config
     */
	public static void setConfig( Config config ){
		_config = config;
	}

	/**
	 * Returns Notification Specific Conf Object
	 * @return
	 * @throws Exception
     */
	public static ConfigObject getNotificationConfigObj() throws Exception
	{
		if( _config.getObject("eagleNotificationProps") == null )
			throw new Exception("Eagle Notification Properties not found in application.conf ");
		return _config.getObject("eagleNotificationProps");
	}

	/**
	 * Fetch Notification specific property value
	 * @param key
	 * @return
	 * @throws Exception
     */
	public static String getPropValue(String key ) throws Exception {

		if (_conf == null)
			_conf = getNotificationConfigObj();
		return _conf.get(key).unwrapped().toString();
	}

	/**
	 * Find all Active Alerts
	 * @return
	 * @throws Exception
     */
	public static List<AlertDefinitionAPIEntity> fetchActiveAlerts() throws Exception {
		List<AlertDefinitionAPIEntity> result = null;
		String site = _config.getString("eagleProps.site");
		String dataSource = _config.getString("eagleProps.dataSource");
		PolicyDefinitionDAO policyDefinitionDao = new PolicyDefinitionEntityDAOImpl(new EagleServiceConnector(_config) , Constants.ALERT_DEFINITION_SERVICE_ENDPOINT_NAME);
		try{
			 result = policyDefinitionDao.findActivePolicies(site, dataSource );
		}catch (Exception ex ){
			throw  new Exception(ex.getMessage());
		}
		return result;
	}

	/**
	 * Deserialize Notification Definition and convert all config to Key Value Pairs
	 * @param notificationDef
	 * @return
	 * @throws Exception
     */
	public static List<Map<String,String>> deserializeNotificationConfig( String notificationDef ) throws Exception {
		List<Map<String, String>> configList = null;
		try{
			configList = mapper.readValue( notificationDef , mapCollectionType);
		}catch (Exception ex ){
			throw  new Exception( ex.getMessage());
		}
		return configList;
	}

	/**
	 * Object to JSON String
	 * @param obj
	 * @return
	 * @throws Exception
     */
	public static String objectToStr( Object obj ) throws  Exception {
		String result = null;
		try{
			result = mapper.writeValueAsString(obj);
		}catch (Exception ex ){
			throw  new Exception( ex.getMessage());
		}
		return result;
	}
}