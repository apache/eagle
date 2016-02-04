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
import org.apache.eagle.alert.entity.AlertDefinitionAPIEntity;
import org.apache.eagle.alert.entity.AlertNotificationEntity;
import org.apache.eagle.common.config.EagleConfigFactory;
import org.apache.eagle.notification.dao.AlertNotificationDAO;
import org.apache.eagle.notification.dao.AlertNotificationDAOImpl;
import org.apache.eagle.service.client.EagleServiceConnector;
import org.reflections.Reflections;
import org.reflections.util.ClasspathHelper;
import org.reflections.util.ConfigurationBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Resource;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Plugin loader which scans the entire code base and register them
 */
public class NotificationPluginLoader {

    private static final Logger LOG = LoggerFactory.getLogger(NotificationPluginLoader.class);
    private static Map<String,Object> notificationMapping = new ConcurrentHashMap<String,Object>();
    private static NotificationPluginLoader _loader = new NotificationPluginLoader();

    private NotificationPluginLoader(){
        loadPlugins();
    }

    public static NotificationPluginLoader  getInstance(){

        if( _loader == null ) synchronized (NotificationPluginLoader.class) {
            if (_loader == null)
                _loader = new NotificationPluginLoader();
        }
        return  _loader;
    }
    /**
     * Scan & Load Plugins
     */
    public void loadPlugins(){
        try {
            LOG.info(" Start loading Plugins ");
            notificationMapping.clear();
            Config config = EagleConfigFactory.load().getConfig();
            AlertNotificationDAO dao = new AlertNotificationDAOImpl(new EagleServiceConnector(config));
            dao.deleteAllAlertNotifications();
            dao.persistAlertNotificationTypes( buildAlertNotificationEntities() );
            dao = null;
            LOG.info("Notification Plugins loaded successfully..");
        }catch ( Exception ex ){
            LOG.error(" Error in loading Notification Plugins . Reason : "+ex.getMessage());

        }
    }


    /**
     * Scan Notification Plugins
     */
    private  Set<Class<? extends NotificationPlugin>> scanNotificationPlugins() {
        Set<Class<? extends NotificationPlugin>> subTypes = new HashSet<Class<? extends NotificationPlugin>>();
        try{
            LOG.info(" Scanning all classes which implements NotificationPlugin Interface ");
            Reflections reflections = new Reflections(new ConfigurationBuilder().setUrls(ClasspathHelper.forJavaClassPath()));
            subTypes = reflections.getSubTypesOf(NotificationPlugin.class);
            LOG.info(" No of Plugins found : "+subTypes.size() );
            if( subTypes.size() <= 0 )
                throw  new Exception(" Notifications API not found in jar ");
        }
        catch ( Exception ex ){
            LOG.error(" Error in Scanning Plugins using Reflection API . Reason : "+ex.getMessage());
        }
        return  subTypes;
    }

    /**
     * Scan and find out list of available Notification Plugins & Its Types
     * @return
     */
    private  List<String> findNotificationTypes()
    {
        List<String> result = new ArrayList<String>();
        Set<Class<? extends NotificationPlugin>> subTypes = scanNotificationPlugins();
        for( Class<? extends NotificationPlugin> clazz: subTypes  ){
            try{
                String notificationType = clazz.getAnnotation(Resource.class).name();
                if( null != notificationType ) {
                    result.add(notificationType);
                    notificationMapping.put( notificationType, clazz.newInstance() );
                }else
                    LOG.info(" OOPs Something wrong in Notification Plugin Impl , Looks like Resource Annotation is missing ");
            }catch (Exception ex ){
                LOG.error(" Error in determining notification types. Reason : "+ex.getMessage() );
            }
        }
        return result;
    }

    /**
     * Build Entity to persist Alert Notification Types
     * @return
     */
    private List<AlertNotificationEntity>  buildAlertNotificationEntities()
    {
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

    public Map<String, Object> getNotificationMapping() {
        return notificationMapping;
    }
}