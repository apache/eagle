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
import org.apache.eagle.alert.entity.AlertNotificationEntity;
import org.apache.eagle.common.config.EagleConfigFactory;
import org.apache.eagle.notification.dao.AlertNotificationDAO;
import org.apache.eagle.notification.dao.AlertNotificationDAOImpl;
import org.apache.eagle.service.client.EagleServiceConnector;
import org.reflections.Reflections;

import javax.annotation.Resource;
import java.util.*;

/**
 * Plugin loader which scans the entire code base and register them
 */
public class NotificationPluginLoader {
    public  static Map<String,Object> notificationMapping = new HashMap<String,Object>();
    public static boolean isPluginLoaded = false;

    static {
        notificationMapping.clear();
        loadPlugins();
    }

    /**
     * Scan & Load Plugins
     */
    public static void loadPlugins(){
        // TO DO
        // Find all Notification Types
        // Clear all in DB Store
        // ReCreate all Notification Types
        try {
            notificationMapping.clear();
            Config config = EagleConfigFactory.load().getConfig();
            AlertNotificationDAO dao = new AlertNotificationDAOImpl(new EagleServiceConnector(config.getString("eagleProps.eagleService.host"), config.getInt("eagleProps.eagleService.port")));
            dao.deleteAllAlertNotifications();
            dao.persistAlertNotificationTypes( buildAlertNotificationEntities() );
            isPluginLoaded = true;
        }catch ( Exception ex ){
            ex.printStackTrace();
        }
    }

    /**
     * Scan Notification Plugins
     */
    public static Set<Class<? extends NotificationPlugin>> scanNotificationPlugins(){
        Reflections reflections = new Reflections("");
        Set<Class<? extends NotificationPlugin>> subTypes = reflections.getSubTypesOf(NotificationPlugin.class);
        return  subTypes;
    }

    /**
     * Scan and find out list of available Notification Plugins & Its Types
     * @return
     */
    public static List<String> findNotificationTypes()
    {
        List<String> result = new ArrayList<String>();
        Set<Class<? extends NotificationPlugin>> subTypes = scanNotificationPlugins();
        for( Class<? extends NotificationPlugin> clazz: subTypes  ){
            try{
                String notificationType = clazz.getAnnotation(Resource.class).name();
                if( null != notificationType ) {
                    result.add(notificationType);
                    notificationMapping.put(notificationType, clazz.newInstance());
                }
            }catch (Exception ex ){

            }
        }
        return result;
    }

    /**
     * Build Entity to persist Alert Notification Types
     * @return
     */
    public static List<AlertNotificationEntity>  buildAlertNotificationEntities() {
        List<AlertNotificationEntity> result = new ArrayList<>();
        List<String> notificationTypes = findNotificationTypes();
        for (String notificationType : notificationTypes) {
            AlertNotificationEntity entity = new AlertNotificationEntity();
            Map<String, String> tags = new HashMap<String, String>();
            tags.put("notificationType", notificationType);
            entity.setEnabled(true);
            entity.setTags(tags);
            result.add(entity);
        }
        return result;
    }

}