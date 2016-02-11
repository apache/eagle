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
import org.apache.eagle.notification.NotificationConstants;
import org.apache.eagle.notification.NotificationPlugin;
import org.apache.eagle.notification.utils.NotificationPluginUtils;
import org.apache.eagle.policy.common.Constants;
import org.apache.eagle.policy.dao.PolicyDefinitionDAO;
import org.apache.eagle.policy.dao.PolicyDefinitionEntityDAOImpl;
import org.apache.eagle.service.client.EagleServiceConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created on 2/10/16.
 */
public class NewNotificationPluginManagerImpl implements NewNotificationPluginManager{
    private static final Logger LOG = LoggerFactory.getLogger(NewNotificationPluginManagerImpl.class);
    // mapping from policy Id to NotificationPlugin instance
    private Map<String, List<NotificationPlugin>> policyNotificationMapping = new ConcurrentHashMap<>(1); //only one write thread
    private Config config;

    public NewNotificationPluginManagerImpl(Config config){
        this.config = config;
        internalInit();
    }

    private void internalInit(){
        // iterate all policy ids, keep those notification which belong to plugins
        PolicyDefinitionDAO policyDefinitionDao = new PolicyDefinitionEntityDAOImpl(new EagleServiceConnector( config ) , Constants.ALERT_DEFINITION_SERVICE_ENDPOINT_NAME);
        String site = config.getString("eagleProps.site");
        String dataSource = config.getString("eagleProps.dataSource");
        try{
            List<AlertDefinitionAPIEntity> activeAlertDefs = policyDefinitionDao.findActivePolicies( site , dataSource );
            for( AlertDefinitionAPIEntity entity : activeAlertDefs ){
                List<NotificationPlugin> plugins = pluginsForPolicy(entity);
                policyNotificationMapping.put(entity.getTags().get(Constants.POLICY_ID) , plugins);
            }
        }catch (Exception ex ){
            LOG.error("Error initializing poliy/notification mapping ", ex);
            throw new IllegalStateException(ex);
        }
    }

    @Override
    public void notifyAlert(AlertAPIEntity entity) {
        String policyId = entity.getTags().get(Constants.POLICY_ID);
        List<NotificationPlugin> plugins = policyNotificationMapping.get(policyId);
        if(plugins == null || plugins.size() == 0) {
            LOG.debug("no plugin found for policy " + policyId);
            return;
        }
        for(NotificationPlugin plugin : plugins){
            try {
                plugin.onAlert(entity);
            }catch(Exception ex){
                LOG.error("fail invoking plugin's onAlert, continue ", ex);
            }
        }
    }

    @Override
    public void updateNotificationPlugins(AlertDefinitionAPIEntity entity, boolean isDelete) {
        String policyId = entity.getTags().get(Constants.POLICY_ID);
        if(isDelete){
            policyNotificationMapping.remove(policyId);
        }else{
            try {
                policyNotificationMapping.put(policyId, pluginsForPolicy(entity));
            }catch(Exception ex){
                LOG.warn("fail updating policyNotificationMapping, but continue to run", ex);
            }
        }
    }

    private List<NotificationPlugin> pluginsForPolicy(AlertDefinitionAPIEntity policy) throws Exception{
        NewNotificationPluginLoader loader = NewNotificationPluginLoader.getInstance();
        loader.init(config);
        Map<String, NotificationPlugin> plugins = loader.getNotificationMapping();
        List<NotificationPlugin>  notifications = new ArrayList<>();
        List<Map<String,String>> notificationConfigCollection = NotificationPluginUtils.deserializeNotificationConfig(policy.getNotificationDef());
        for( Map<String,String> notificationConf : notificationConfigCollection ){
            String notificationType = notificationConf.get(NotificationConstants.NOTIFICATION_TYPE);
            if(!plugins.containsKey(notificationType)){
                LOG.warn("No NotificationPlugin supports this notificationType " + notificationType);
            }else {
                notifications.add(plugins.get(notificationType));
            }
        }
        return notifications;
    }
}
