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
public class NotificationPluginLoader {
    private static final Logger LOG = LoggerFactory.getLogger(NotificationPluginLoader.class);
    private static NotificationPluginLoader instance = new NotificationPluginLoader();
    private static Map<String,NotificationPlugin> notificationMapping = new ConcurrentHashMap<>();

    private Config config;
    private boolean initialized = false;

    public static NotificationPluginLoader getInstance(){
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
            LOG.info("Start loading Plugins from eagle service ...");
            AlertNotificationDAO dao = new AlertNotificationDAOImpl(new EagleServiceConnector(config));
            List<AlertNotificationEntity> activeNotificationPlugins = dao.findAlertNotificationTypes();
            for(AlertNotificationEntity plugin : activeNotificationPlugins){
                notificationMapping.put(plugin.getTags().get(NotificationConstants.NOTIFICATION_TYPE),
                        (NotificationPlugin) Class.forName(plugin.getClassName()).newInstance());
            }
            LOG.info("successfully loaded Plugins from eagle service " + activeNotificationPlugins);
        }catch ( Exception ex ){
            LOG.error("Error in loading Notification Plugins: ", ex);
            throw new IllegalStateException(ex);
        }
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
