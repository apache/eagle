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
import org.apache.eagle.alert.entity.AlertNotificationEntity;
import org.apache.eagle.notification.base.NotificationConstants;
import org.apache.eagle.notification.dao.AlertNotificationDAO;
import org.apache.eagle.notification.dao.AlertNotificationDAOImpl;
import org.apache.eagle.service.client.EagleServiceConnector;
import org.reflections.Reflections;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created on 2/10/16.
 * don't support dynamic discovery as of 2/10
 */
public class NewNotificationPluginLoader {
    private static final Logger LOG = LoggerFactory.getLogger(NewNotificationPluginLoader.class);
    private static NewNotificationPluginLoader instance = new NewNotificationPluginLoader();
    private static Map<String,NewNotificationPlugin> notificationMapping = new ConcurrentHashMap<>();

    private Config config;
    private boolean initialized = false;

    public static NewNotificationPluginLoader getInstance(){
        return instance;
    }

    public void init(Config config){
        if(!initialized){
            synchronized (this){
                if(!initialized){
                    internalInit(config);
                    initialized = true;
                }
            }
        }
    }

    private void internalInit(Config config){
        this.config = config;
        loadPlugins();
    }

    /**
     * Scan & Load Plugins
     */
    private void loadPlugins(){
        try {
            LOG.info(" Start loading Plugins ");
            Set<Class<? extends NewNotificationPlugin>> subTypes = scanNotificationPlugins();
            List<AlertNotificationEntity> result = new ArrayList<>();
            for( Class<? extends NewNotificationPlugin> clazz: subTypes  ){
                NewNotificationPlugin plugin = clazz.newInstance();
                String notificationType = plugin.getMetadata().name;
                if( null != notificationType ) {
                    AlertNotificationEntity entity = new AlertNotificationEntity();
                    Map<String, String> tags = new HashMap<>();
                    tags.put(NotificationConstants.NOTIFICATION_TYPE, notificationType);
                    entity.setEnabled(true);
                    entity.setTags(tags);
                    result.add(entity);
                    notificationMapping.put( notificationType, clazz.newInstance() );
                }else {
                    LOG.error("Something wrong in Notification Plugin Impl , Looks like Resource Annotation is missing, ignoring this plugin and continue ");
                }
            }

            AlertNotificationDAO dao = new AlertNotificationDAOImpl(new EagleServiceConnector(config));
            dao.deleteAllAlertNotifications();
            dao.persistAlertNotificationTypes( result );
            LOG.info("Notification Plugins loaded successfully..");
        }catch ( Exception ex ){
            LOG.error("Error in loading Notification Plugins: ", ex);
            throw new IllegalStateException(ex);
        }
    }

    /**
     * Scan Notification Plugins
     */
    private  Set<Class<? extends NewNotificationPlugin>> scanNotificationPlugins() {
        Set<Class<? extends NewNotificationPlugin>> subTypes = null;
        try{
            LOG.info("Scanning all classes which implements NotificationPlugin Interface ");
            Reflections reflections = new Reflections();
            subTypes = reflections.getSubTypesOf(NewNotificationPlugin.class);
            LOG.info("Number of Plugins found : " + subTypes.size() );
            if(subTypes.size() <= 0)
                LOG.warn("Notifications API not found in jar ");
            for(Class<? extends NewNotificationPlugin> pluginCls : subTypes){
                LOG.info("Notification Plugin class " + pluginCls.getName());
            }
        }
        catch ( Exception ex ){
            LOG.error(" Error in Scanning Plugins using Reflection API", ex);
            throw new IllegalStateException(ex);
        }
        return  subTypes;
    }

    public Map<String, NewNotificationPlugin> getNotificationMapping() {
        ensureInitialized();
        return notificationMapping;
    }

    private void ensureInitialized(){
        if(!initialized)
            throw new IllegalStateException("Plugin loader not initialized");
    }
}
