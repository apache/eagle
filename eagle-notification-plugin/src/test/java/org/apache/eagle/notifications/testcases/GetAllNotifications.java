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
import org.apache.eagle.alert.entity.AlertNotificationEntity;
import org.apache.eagle.common.config.EagleConfigFactory;
import org.apache.eagle.notification.dao.AlertNotificationDAO;
import org.apache.eagle.notification.dao.AlertNotificationDAOImpl;
import org.apache.eagle.service.client.EagleServiceConnector;
import org.junit.Test;

import java.util.List;

/**
 *
 */
public class GetAllNotifications {

    @Test
    public void  getAllNotification() throws Exception {
        Config config = EagleConfigFactory.load().getConfig();
        AlertNotificationDAO dao = new AlertNotificationDAOImpl( new EagleServiceConnector(config));
        List<AlertNotificationEntity> list = dao.findAlertNotificationTypes();
        System.out.println(" Fetch all Notifications : "+list);
    }
}
