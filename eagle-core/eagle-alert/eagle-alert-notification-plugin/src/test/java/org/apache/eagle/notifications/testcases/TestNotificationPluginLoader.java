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
package org.apache.eagle.notifications.testcases;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import junit.framework.Assert;
import org.apache.eagle.notification.base.NotificationConstants;
import org.apache.eagle.notification.plugin.NotificationPluginLoader;
import org.junit.Ignore;
import org.junit.Test;

/**
 * Created on 2/10/16.
 */
public class TestNotificationPluginLoader {
    @Ignore //only work when connected to eagle service
    @Test
    public void testLoader(){
        Config config = ConfigFactory.load();
        NotificationPluginLoader loader = NotificationPluginLoader.getInstance();
        loader.init(config);
        Assert.assertTrue(loader.getNotificationMapping().keySet().contains(NotificationConstants.EAGLE_STORE));
        Assert.assertTrue(loader.getNotificationMapping().keySet().contains(NotificationConstants.KAFKA_STORE));
        Assert.assertTrue(loader.getNotificationMapping().keySet().contains(NotificationConstants.EMAIL_NOTIFICATION));
    }
}
