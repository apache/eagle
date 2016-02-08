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

import com.sun.tools.internal.jxc.apt.Const;
import com.typesafe.config.Config;
import org.apache.commons.collections.CollectionUtils;
import org.apache.eagle.alert.entity.AlertAPIEntity;
import org.apache.eagle.alert.entity.AlertDefinitionAPIEntity;
import org.apache.eagle.common.config.EagleConfigFactory;
import org.apache.eagle.notification.utils.NotificationPluginUtils;
import org.apache.eagle.policy.common.Constants;
import org.apache.eagle.policy.dao.PolicyDefinitionDAO;
import org.apache.eagle.policy.dao.PolicyDefinitionEntityDAOImpl;
import org.apache.eagle.service.client.EagleServiceConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Notification manager that is responsible for
 * <p> Scanning Plugins </p>
 * <p> Loading Plugins and Policy Mapping </p>
 * <p> Initializing Plugins </p>
 * <p> Forwarding eagle alert to configured Notification Plugin </p>
 * <p> BroadCast Changes in Policy to all Notification Plugins  </p>
 */
public class NotificationManager  {

    private static final Logger LOG = LoggerFactory.getLogger(NotificationManager.class);
    private static Map<String, Set<String>> policyNotificationMapping = new ConcurrentHashMap<String,Set<String> >();
    private static NotificationManager notificationManager;
    private Config _conf;

    private NotificationManager( Config config ) throws  Exception {
        this._conf = config;
        // initialize Manger here
        _init();
    }

    /**
     * Create Single Instance of Object
     * @return
     */
    public  static NotificationManager getInstance( Config config ) throws  Exception {
        if( notificationManager == null ) {
            synchronized (NotificationManager.class) {
                if (notificationManager == null)
                    notificationManager = new NotificationManager(config);
            }
        }
        return notificationManager;
    }

    /**
     * Initialization of Notification Manager
     */
    private void _init() throws  Exception {
        policyNotificationMapping.clear();
        //  Pass Config and set to all API's
        NotificationPluginUtils.setConfig(this._conf);
        Set<String> plugins = NotificationPluginLoader.getInstance( this._conf ).getNotificationMapping().keySet();
        for( String plugin : plugins ){
            try {
                Object obj =  NotificationPluginLoader.getInstance( this._conf ).getNotificationMapping().get(plugin);
                obj.getClass().getMethod("_init" , new Class[]{Config.class}).invoke(obj , this._conf); // invoke _init method of all notification plugins
            } catch (Exception e) {
                LOG.error(" Error in loading Plugins . Reason : "+e.getMessage());
                throw new Exception(e);
            }
        }
        String site = _conf.getString("eagleProps.site");
        String dataSource = _conf.getString("eagleProps.dataSource");
        // find notification Types
        PolicyDefinitionDAO  policyDefinitionDao = new PolicyDefinitionEntityDAOImpl(new EagleServiceConnector( _conf ) , Constants.ALERT_DEFINITION_SERVICE_ENDPOINT_NAME);
        try{
            List<AlertDefinitionAPIEntity> activeAlerts = policyDefinitionDao.findActivePolicies( site , dataSource );
            for( AlertDefinitionAPIEntity entity : activeAlerts ){
                List<Map<String,String>> notificationConfigCollection = NotificationPluginUtils.deserializeNotificationConfig(entity.getNotificationDef());
                Set<String>  notifications = new HashSet<String>();
                for( Map<String,String> notificationConf : notificationConfigCollection ){
                    notifications.add(notificationConf.get(NotificationConstants.NOTIFICATION_TYPE));
                }
                policyNotificationMapping.put(entity.getTags().get(Constants.POLICY_ID) , notifications );
            }
        }catch (Exception ex ){
            LOG.error(" Error in determining policy and its notification type. Reason : "+ex.getMessage());
        }
    }

    /**
     * To Pass Alert to respective Notification Plugins and notify to configured system
     * @param entity
     */
    public void notifyAlert( AlertAPIEntity entity ) {
        try {
            Set<String> listOfNotifications = policyNotificationMapping.get(entity.getTags().get(Constants.POLICY_ID));
            if( listOfNotifications == null || listOfNotifications.size() <= 0 ){
                LOG.info("Notifications List is Null or Empty .. Policy might be deleted . Policy Id : "+entity.getTags().get(Constants.POLICY_ID));
                return;
            }
            LOG.info(" Invoking Notification Plugin for the Policy : "+entity.getTags().get(Constants.POLICY_ID)+" . No of Plugins found : "+listOfNotifications.size());
            for(String notificationType : listOfNotifications ){
                Object notificationPluginObj = getNotificationPluginAPI(notificationType);
                if( notificationPluginObj == null )
                    throw  new Exception(" Notification API is NULL .. Looks like Notification Plugins not loaded Properly ..  " +
                            "Notifcation Type : "+notificationType +" Available Notifications : "+NotificationPluginLoader.getInstance(this._conf).getNotificationMapping());
                notificationPluginObj.getClass().getMethod("onAlert", new Class[]{AlertAPIEntity.class}).invoke(notificationPluginObj, entity);
            }
            LOG.info(" Successfully Notified ...");
        } catch ( Exception ex) {
            LOG.error(" Error in NotificationManager when invoking notifyAlert method  . Reason : "+ex.getMessage());
        }
    }

    /**
     * Returns Notification Plugin for the given Type
     * @param type
     * @return
     */
    private Object getNotificationPluginAPI( String type ){
        return NotificationPluginLoader.getInstance(this._conf).getNotificationMapping().get(type);
    }

    /**
     * Update all Notification Plugin if changes in Policy
     * @param entity
     */
    public void updateNotificationPlugins( AlertDefinitionAPIEntity entity , boolean isDeleteUpdate ){
        try {
            // Update Notification Plugin about the change in AlertDefinition
            String policyId = entity.getTags().get(Constants.POLICY_ID);
            List<Map<String,String>> notificationConfigCollection = NotificationPluginUtils.deserializeNotificationConfig(entity.getNotificationDef());
            Set<String>  notifications = new HashSet<String>();
            for( Map<String,String> notificationConf : notificationConfigCollection ) {
                String notificationType = notificationConf.get(NotificationConstants.NOTIFICATION_TYPE);
                if( !NotificationPluginLoader.getInstance(this._conf).getNotificationMapping().containsKey(notificationType)){
                    LOG.error(" Can't find Notification Type in Plugin Loader.. OOPS! Something went Wrong ");
                }
                // add policy id to config map , all plugins need policy id for maintaining their config obj
                notificationConf.put(Constants.POLICY_ID, policyId );
                try{
                    Object  notificationObj = NotificationPluginLoader.getInstance(this._conf).getNotificationMapping().get(notificationType);
                    notificationObj.getClass().getMethod("update" , new Class[]{Map.class,boolean.class}).invoke( notificationObj , notificationConf , isDeleteUpdate );
                }catch (Exception ex ){
                    LOG.error(" Error in Updating Notification Config to Plugin , Policy Id : "+entity.getTags().get(Constants.POLICY_ID)+" .. Reason : "+ex.getMessage());
                }
                notifications.add(notificationType);
            }
            if( isDeleteUpdate ){ // make sure to remove it from policyNotificationMapping
                policyNotificationMapping.remove(policyId);
            }else
                policyNotificationMapping.put(policyId, notifications);// update policy - notification types map

            LOG.info(" Successfully broad casted policy updates to all Notification Plugins ...");
        } catch (Exception e) {
            LOG.error(" Error in updateNotificationPlugins  . Reason : "+e.getMessage());
        }
    }
}