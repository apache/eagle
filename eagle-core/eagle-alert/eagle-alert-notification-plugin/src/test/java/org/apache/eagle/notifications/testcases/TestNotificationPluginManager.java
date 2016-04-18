/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

package org.apache.eagle.notifications.testcases;


import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.eagle.alert.entity.AlertAPIEntity;
import org.apache.eagle.alert.entity.AlertDefinitionAPIEntity;
import org.apache.eagle.common.metric.AlertContext;
import org.apache.eagle.notification.plugin.NotificationPluginManager;
import org.apache.eagle.notification.plugin.NotificationPluginManagerImpl;
import org.apache.eagle.policy.common.Constants;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.util.HashMap;

public class TestNotificationPluginManager {
    @Ignore
    @Test
    public void testUpdateNotificationPlugins() {
        boolean isDelete = false;
        AlertDefinitionAPIEntity alertDef = new AlertDefinitionAPIEntity();
        alertDef.setTags(new HashMap<String, String>());
        alertDef.getTags().put(Constants.POLICY_ID, "testPlugin");
        alertDef.setNotificationDef("[]");
        Config config = ConfigFactory.load();
        NotificationPluginManager manager = new NotificationPluginManagerImpl(config);
        manager.updateNotificationPlugins(alertDef, isDelete);
        Assert.assertTrue(true);
    }
    @Ignore
    @Test
    public void testUpdateNotificationPlugins2() {
        boolean isDelete = false;
        AlertDefinitionAPIEntity alertDef = new AlertDefinitionAPIEntity();
        alertDef.setTags(new HashMap<String, String>());
        alertDef.getTags().put(Constants.POLICY_ID, "testEmptyPlugins");
        alertDef.setNotificationDef("[{\"notificationType\":\"eagleStore\"},{\"notificationType\":\"kafka\",\"kafka_broker\":\"sandbox.hortonworks.com:6667\",\"topic\":\"testTopic\"}]");
        Config config = ConfigFactory.load();
        NotificationPluginManager manager = new NotificationPluginManagerImpl(config);
        manager.updateNotificationPlugins(alertDef, isDelete);
        Assert.assertTrue(true);
    }

    @Ignore
    @Test
    public void testUpdateNotificationPluginsWithDelete() {
        boolean isDelete = true;
        AlertDefinitionAPIEntity alertDef = new AlertDefinitionAPIEntity();
        alertDef.setTags(new HashMap<String, String>());
        alertDef.getTags().put(Constants.POLICY_ID, "testEmptyPlugins");
        alertDef.setNotificationDef("[]");
        Config config = ConfigFactory.load();
        NotificationPluginManager manager = new NotificationPluginManagerImpl(config);
        manager.updateNotificationPlugins(alertDef, isDelete);
        Assert.assertTrue(true);
    }

    @Ignore
    @Test
    public void testMultipleNotificationInstance() {
        AlertAPIEntity alert = new AlertAPIEntity();
        alert.setTags(new HashMap<String, String>());
        alert.getTags().put(Constants.POLICY_ID, "testPlugin");
        alert.setDescription("");
        alert.setAlertContext(new AlertContext().toJsonString());

        Config config = ConfigFactory.load();
        NotificationPluginManager manager = new NotificationPluginManagerImpl(config);
        manager.notifyAlert(alert);
        Assert.assertTrue(true);
    }

}
