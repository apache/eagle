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
import org.apache.eagle.alert.entity.AlertAPIEntity;
import org.apache.eagle.alert.entity.AlertDefinitionAPIEntity;
import org.apache.eagle.notification.plugin.AlertEagleStorePlugin;
import org.junit.Ignore;
import org.junit.Test;

import java.util.Arrays;

/**
 * Created on 2/11/16.
 */
public class TestAlertEagleStorePlugin {
    @Ignore // only work when eagle service is up
    @Test
    public void testEagleStorePlugin() throws Exception{
        AlertEagleStorePlugin plugin = new AlertEagleStorePlugin();
        Config config = ConfigFactory.load();
        AlertDefinitionAPIEntity def = new AlertDefinitionAPIEntity();
        def.setNotificationDef("");
        plugin.init(config, Arrays.asList(def));

        AlertAPIEntity alert = new AlertAPIEntity();
        alert.setDescription("");
        plugin.onAlert(alert);
        Assert.assertTrue(plugin.getStatus().successful);
    }
}
