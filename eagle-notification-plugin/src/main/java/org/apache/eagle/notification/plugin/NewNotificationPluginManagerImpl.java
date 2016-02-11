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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created on 2/10/16.
 */
public class NewNotificationPluginManagerImpl implements NewNotificationPluginManager{
    private static final Logger LOG = LoggerFactory.getLogger(NewNotificationPluginManagerImpl.class);
    private Map<String, Set<String>> policyNotificationMapping = new ConcurrentHashMap<String,Set<String> >(1); //only one write thread

    private Config config;
    public NewNotificationPluginManagerImpl(Config config){
        this.config = config;
        init();
    }

    private void init(){

    }

    @Override
    public void notifyAlert(AlertAPIEntity entity) {

    }

    @Override
    public void updateNotificationPlugins(AlertDefinitionAPIEntity entity, boolean isDelete) {

    }
}
