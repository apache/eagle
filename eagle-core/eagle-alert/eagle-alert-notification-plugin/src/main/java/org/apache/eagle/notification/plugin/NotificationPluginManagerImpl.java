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
import org.apache.commons.collections.CollectionUtils;
import org.apache.eagle.alert.entity.AlertAPIEntity;
import org.apache.eagle.alert.entity.AlertDefinitionAPIEntity;
import org.apache.eagle.notification.base.NotificationConstants;
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
public class NotificationPluginManagerImpl implements NotificationPluginManager {
    private static final Logger LOG = LoggerFactory.getLogger(NotificationPluginManagerImpl.class);
    // mapping from policy Id to NotificationPlugin instance
    private Map<String, Collection<NotificationPlugin>> policyNotificationMapping = new ConcurrentHashMap<>(1); //only one write thread
    private Config config;

    public NotificationPluginManagerImpl(Config config){
        this.config = config;
        internalInit();
    }

    private void internalInit(){
        // iterate all policy ids, keep those notification which belong to plugins
        PolicyDefinitionDAO policyDefinitionDao = new PolicyDefinitionEntityDAOImpl(new EagleServiceConnector( config ) , Constants.ALERT_DEFINITION_SERVICE_ENDPOINT_NAME);
        String site = config.getString("eagleProps.site");
        String application = config.getString("eagleProps.application");
        try{
            List<AlertDefinitionAPIEntity> activeAlertDefs = policyDefinitionDao.findActivePolicies( site , application);
            // initialize all loaded plugins
            NotificationPluginLoader.getInstance().init(config);
            for(NotificationPlugin plugin : NotificationPluginLoader.getInstance().getNotificationMapping().values()){
                plugin.init(config, activeAlertDefs);
            }
            // build policy and plugin mapping
            for( AlertDefinitionAPIEntity entity : activeAlertDefs ){
                Map<String, NotificationPlugin> plugins = pluginsForPolicy(entity);
                policyNotificationMapping.put(entity.getTags().get(Constants.POLICY_ID) , plugins.values());
            }
        }catch (Exception ex ){
            LOG.error("Error initializing policy/notification mapping ", ex);
            throw new IllegalStateException(ex);
        }
    }

    @Override
    public void notifyAlert(AlertAPIEntity entity) {
        String policyId = entity.getTags().get(Constants.POLICY_ID);
        Collection<NotificationPlugin> plugins = policyNotificationMapping.get(policyId);
        if(plugins == null || plugins.size() == 0) {
            LOG.warn("no alert notification plugins found for policy " + policyId);
            return;
        }
        for(NotificationPlugin plugin : plugins){
            try {
                LOG.info("execute notification plugin " + plugin);
                plugin.onAlert(entity);
            }catch(Exception ex){
                LOG.error("fail invoking plugin's onAlert, continue ", ex);
            }
        }
    }

    @Override
    public void updateNotificationPlugins(AlertDefinitionAPIEntity alertDef, boolean isDelete) {
        try {
            // Update Notification Plugin about the change in AlertDefinition
            String policyId = alertDef.getTags().get(Constants.POLICY_ID);
            if(isDelete){
                // iterate all plugins and delete this policy
                for(NotificationPlugin plugin : policyNotificationMapping.get(policyId)){
                    plugin.update(policyId, null, true);
                }
                policyNotificationMapping.remove(policyId);
                LOG.info("Deleted notifications for policy " + policyId);
                return;
            }

            Map<String, NotificationPlugin> plugins = pluginsForPolicy(alertDef);
            // calculate difference between current plugins and previous plugin
            Collection<NotificationPlugin> previousPlugins = policyNotificationMapping.get(policyId);
            if(previousPlugins != null) {
                Collection<NotificationPlugin> deletedPlugins = CollectionUtils.subtract(previousPlugins, plugins.values());
                LOG.info("Going to delete plugins " + deletedPlugins + ", for policy " + policyId);
                for (NotificationPlugin plugin : deletedPlugins) {
                    plugin.update(policyId, null, true);
                }
            }

            // iterate current notifications and update it individually
            List<Map<String,String>> notificationConfigCollection = NotificationPluginUtils.deserializeNotificationConfig(alertDef.getNotificationDef());
            for(NotificationPlugin plugin: plugins.values()) {
                plugin.update(policyId, notificationConfigCollection, false);
            }

            policyNotificationMapping.put(policyId, plugins.values());// update policy - notification types map
            LOG.info("Successfully broadcast policy updates to all Notification Plugins ...");
        } catch (Exception e) {
            LOG.error("Error broadcasting policy notification changes ", e);
        }
    }

    private Map<String, NotificationPlugin> pluginsForPolicy(AlertDefinitionAPIEntity policy) throws Exception{
        NotificationPluginLoader loader = NotificationPluginLoader.getInstance();
        loader.init(config);
        Map<String, NotificationPlugin> plugins = loader.getNotificationMapping();
        // mapping from notificationType to plugin
        Map<String, NotificationPlugin>  notifications = new HashMap<>();
        List<Map<String,String>> notificationConfigCollection = NotificationPluginUtils.deserializeNotificationConfig(policy.getNotificationDef());
        for(Map<String,String> notificationConf : notificationConfigCollection ){
            String notificationType = notificationConf.get(NotificationConstants.NOTIFICATION_TYPE);
            // for backward compatibility, by default notification type is email if notification type is not specified
            if(notificationType == null){
                LOG.warn("notificationType is null so use default notification type email for this policy  " + policy);
                notifications.put(NotificationConstants.EMAIL_NOTIFICATION, plugins.get(NotificationConstants.EMAIL_NOTIFICATION));
                notifications.put(NotificationConstants.EAGLE_STORE, plugins.get(NotificationConstants.EAGLE_STORE));
            }else if(!plugins.containsKey(notificationType)){
                LOG.warn("No NotificationPlugin supports this notificationType " + notificationType);
            }else {
                notifications.put(notificationType, plugins.get(notificationType));
            }
        }
        return notifications;
    }
}
