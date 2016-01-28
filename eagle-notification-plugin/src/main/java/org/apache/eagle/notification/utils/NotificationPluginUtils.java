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


import com.typesafe.config.Config;
import com.typesafe.config.ConfigObject;
import org.apache.eagle.common.config.EagleConfigFactory;

public class NotificationPluginUtils {

	public static ConfigObject _conf;
	
	public static ConfigObject getNotificationConfigObj() throws Exception
	{
		Config config = EagleConfigFactory.load().getConfig();
		if( config.getObject("eagleNotificationProps") == null )
			throw new Exception("Eagle Notification Properties not found in application.conf ");
		return config.getObject("eagleNotificationProps");
	}
		
	public static String getPropValue(String key ) throws Exception {

		if (_conf == null)
			_conf = getNotificationConfigObj();
		return _conf.get(key).unwrapped().toString();
	}
}
