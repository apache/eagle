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
import org.apache.eagle.notification.NotificationPlugin;
import org.apache.eagle.notification.dao.AlertNotificationDAO;
import org.apache.eagle.notification.dao.AlertNotificationDAOImpl;
import org.apache.eagle.service.client.EagleServiceConnector;
import org.reflections.Reflections;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Resource;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created on 2/10/16.
 * don't support dynamic discovery
 */
public class NewNotificationPluginLoader {
    private static final Logger LOG = LoggerFactory.getLogger(NewNotificationPluginLoader.class);
    private static NewNotificationPluginLoader instance = new NewNotificationPluginLoader();
    private static Map<String,NotificationPlugin> notificationMapping = new ConcurrentHashMap<String,NotificationPlugin>();

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
            Set<Class<? extends NotificationPlugin>> subTypes = scanNotificationPlugins();
            List<AlertNotificationEntity> result = new ArrayList<AlertNotificationEntity>();
            for( Class<? extends NotificationPlugin> clazz: subTypes  ){
                try{
                    String notificationType = clazz.getAnnotation(Resource.class).name();
                    if( null != notificationType ) {
                        AlertNotificationEntity entity = new AlertNotificationEntity();
                        Map<String, String> tags = new HashMap<String, String>();
                        tags.put("notificationType", notificationType);
                        entity.setEnabled(true);
                        entity.setTags(tags);
                        result.add(entity);
                        notificationMapping.put( notificationType, clazz.newInstance() );
                    }else {
                        throw new IllegalStateException("OOPs Something wrong in Notification Plugin Impl , Looks like Resource Annotation is missing ");
                    }
                }catch (Exception ex ){
                    LOG.error(" Error in determining notification types", ex);
                }
            }

            AlertNotificationDAO dao = new AlertNotificationDAOImpl(new EagleServiceConnector(config));
            dao.deleteAllAlertNotifications();
            dao.persistAlertNotificationTypes( result );
            LOG.info("Notification Plugins loaded successfully..");
        }catch ( Exception ex ){
            LOG.error("Error in loading Notification Plugins: ", ex);
        }
    }

    /**
     * Scan Notification Plugins
     */
    private  Set<Class<? extends NotificationPlugin>> scanNotificationPlugins() {
        Set<Class<? extends NotificationPlugin>> subTypes = null;
        try{
            LOG.info(" Scanning all classes which implements NotificationPlugin Interface ");
            Reflections reflections = new Reflections();
            subTypes = reflections.getSubTypesOf(NotificationPlugin.class);
            LOG.info("Number of Plugins found : " + subTypes.size() );
            if( subTypes.size() <= 0 )
                throw new IllegalStateException(" Notifications API not found in jar ");
            for(Class<? extends NotificationPlugin> pluginCls : subTypes){
                LOG.info("Notification Plugin class " + pluginCls.getName());
            }
        }
        catch ( Exception ex ){
            LOG.error(" Error in Scanning Plugins using Reflection API", ex);
        }
        return  subTypes;
    }

    public Map<String, NotificationPlugin> getNotificationMapping() {
        ensureInitialized();
        return notificationMapping;
    }

    private void ensureInitialized(){
        if(!initialized)
            throw new IllegalStateException("Plugin loader not initialized");
    }
}
