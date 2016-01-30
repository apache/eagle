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

package org.apache.eagle.notification;

import org.apache.eagle.alert.entity.AlertAPIEntity;
import org.apache.eagle.alert.entity.AlertDefinitionAPIEntity;

import java.util.Map;

/**
 * Notification Plug-in interface which provide abstraction layer to notify to different system
 */
public interface NotificationPlugin {

	/**
	 * for initialization
	 * @throws Exception
     */
	public void _init() throws  Exception;

	/**
	 * Update Plugin if any change in Policy Definition
	 * @param  notificationConf
	 * @throws Exception
     */
	public void update( Map<String,String> notificationConf , boolean isPolicyDelete ) throws  Exception;
	/**
	 * Post a notification for the given alertEntity 
	 * @param alertEntity
	 * @throws Exception
	 */

	public void onAlert( AlertAPIEntity alertEntity ) throws  Exception;
	
	/**
	 * Returns Status of Notification Post 
	 * @return
	 */
	public NotificationStatus getStatus();
}
