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

import com.typesafe.config.Config;
import org.apache.eagle.alert.common.AlertConstants;
import org.apache.eagle.alert.dao.AlertDefinitionDAO;
import org.apache.eagle.alert.dao.AlertDefinitionDAOImpl;
import org.apache.eagle.alert.entity.AlertAPIEntity;
import org.apache.eagle.alert.entity.AlertDefinitionAPIEntity;
import org.apache.eagle.common.config.EagleConfigFactory;
import org.apache.eagle.service.client.EagleServiceConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Notification manager that is responsible for
 * <p> Scanning Plugins </p>
 * <p> Loading Plugins and Policy Mapping </p>
 * <p> Initializing Plugins </p>
 */
public class NotificationManager  {

    public static Map<String, String > policyNotificationMapping = new ConcurrentHashMap<String,String>();
    private static final Logger LOG = LoggerFactory.getLogger(NotificationManager.class);

    /**
     * Static Initializer of Manager
     */
    static {
        policyNotificationMapping.clear();
        // initialize all Notification Plugins
        _init();
    }

    /**
     * Initialization of Notification Manager
     */
    private static void _init() {
        policyNotificationMapping.clear();
        Set<String> plugins = NotificationPluginLoader.notificationMapping.keySet();
        for( String plugin : plugins ){
            try {
                Object obj =  NotificationPluginLoader.notificationMapping.get(plugin);
                obj.getClass().getMethod("_init").invoke(obj);
            } catch (Exception e) {
                LOG.error(" Error in loading Plugins . Reason : "+e.getMessage());
            }
        }
        Config config = EagleConfigFactory.load().getConfig();
        String site = config.getString("eagleProps.site");
        String dataSource = config.getString("eagleProps.dataSource");
        // find notification Types
        AlertDefinitionDAO alertDefinitionDao = new AlertDefinitionDAOImpl(new EagleServiceConnector(config.getString("eagleProps.eagleService.host"), config.getInt("eagleProps.eagleService.port")));
        try{
            List<AlertDefinitionAPIEntity> activeAlerts = alertDefinitionDao.findActiveAlertDefs( site , dataSource);
            for( AlertDefinitionAPIEntity entity : activeAlerts ){
                policyNotificationMapping.put(entity.getTags().get(AlertConstants.POLICY_ID) , entity.getTags().get(AlertConstants.NOTIFICATION_TYPE));
            }
        }catch (Exception ex ){
            LOG.error(ex.getMessage());
        }
    }

    /**
     * To Pass Alert to respective Notification Plugins
     * @param entity
     */
    public void notifyAlert( AlertAPIEntity entity ) {
        try {
            Object obj  = getNotificationPluginAPI( this.policyNotificationMapping.get(entity.getTags().get(AlertConstants.POLICY_ID)) );
            obj.getClass().getMethod("onAlert" , new Class[]{AlertAPIEntity.class}).invoke( obj , entity);
        } catch ( Exception e) {
            LOG.error(" Error in NotificationManager when invoking NotifyAlert method  . Reason : "+e.getMessage());
        }
    }

    /**
     * Returns Notification Plugin for the given Type
     * @param type
     * @return
     */
    private Object getNotificationPluginAPI( String type ){
        return NotificationPluginLoader.notificationMapping.get(type);
    }

    public void updateNotificationPlugins( AlertDefinitionAPIEntity entity ){
        try {
            // Re Load the plugins
            // Re visit this , this should be in better way
           NotificationPluginLoader.loadPlugins();
            // Re initialize Notification Manager
            _init();
        } catch (Exception e) {
            LOG.error(" Error in updateNotificationPlugins  . Reason : "+e.getMessage());
        }
    }

}
