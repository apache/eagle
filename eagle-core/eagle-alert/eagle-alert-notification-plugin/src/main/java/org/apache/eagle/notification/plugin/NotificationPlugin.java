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
package org.apache.eagle.notification.plugin;

import com.typesafe.config.Config;
import org.apache.eagle.alert.entity.AlertAPIEntity;
import org.apache.eagle.alert.entity.AlertDefinitionAPIEntity;
import org.apache.eagle.notification.base.NotificationStatus;

import java.util.List;
import java.util.Map;

/**
 * Created on 2/10/16.
 * Notification Plug-in interface which provide abstraction layer to notify to different system
 */
public interface NotificationPlugin {
    /**
     * for initialization
     * @throws Exception
     */
    public void init(Config config, List<AlertDefinitionAPIEntity> initAlertDefs) throws  Exception;

    /**
     * Update Plugin if any change in Policy Definition
     * @param policy to be impacted
     * @param  notificationConf
     * @throws Exception
     */
    public void update(String policy, Map<String,String> notificationConf , boolean isPolicyDelete ) throws  Exception;

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
