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

package org.apache.eagle.notification.dao;

import org.apache.commons.lang.time.DateUtils;
import org.apache.eagle.alert.entity.AlertNotificationEntity;
import org.apache.eagle.log.entity.GenericServiceAPIResponseEntity;
import org.apache.eagle.policy.common.Constants;
import org.apache.eagle.service.client.EagleServiceConnector;
import org.apache.eagle.service.client.IEagleServiceClient;
import org.apache.eagle.service.client.impl.EagleServiceClientImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Notification Service API implementation which Provides Read/Write API's of Hbase AlertNotifications Table
 */
public class AlertNotificationDAOImpl implements  AlertNotificationDAO {

    private final Logger LOG = LoggerFactory.getLogger(AlertNotificationDAOImpl.class);
    private final EagleServiceConnector connector;

    public AlertNotificationDAOImpl(EagleServiceConnector connector){
        this.connector = connector;
    }

    /**
     * Find the Alerts by NotificationType
     * @return
     * @throws Exception
     */
    @Override
    public List<AlertNotificationEntity> findAlertNotificationTypes() throws Exception {
        try{
            IEagleServiceClient client = new EagleServiceClientImpl(connector);
            String query = Constants.ALERT_NOTIFICATION_SERVICE_ENDPOINT_NAME+"[@enabled=\"true\"]{*}";
            GenericServiceAPIResponseEntity response = client.search(query).startTime(0)
                    .endTime(10 * DateUtils.MILLIS_PER_DAY)
                    .pageSize(Integer.MAX_VALUE)
                    .query(query)
                    .send();
            client.close();
            if (response.getException() != null) {
                throw new Exception("Got an exception when query eagle service: " + response.getException());
            }
            return response.getObj();
        }
        catch (Exception ex) {
            LOG.error("Got an exception when query alert notification service ", ex);
            throw new IllegalStateException(ex);
        }
    }

    /**
     * Delete entire Habse Table
     * @throws Exception
     */
    @Override
    public void deleteAllAlertNotifications() throws Exception {
        IEagleServiceClient client = new EagleServiceClientImpl(connector);
        String query = Constants.ALERT_NOTIFICATION_SERVICE_ENDPOINT_NAME+"[]{*}";
        GenericServiceAPIResponseEntity response = client.delete()
                .byQuery(query)
                .startTime(0).endTime(System.currentTimeMillis()).pageSize(1000).send();
        if(!response.isSuccess())
            throw new Exception(" Alert Notification Entities deletion failed ");
    }

    /**
     * Persist AlertNotification Entites
     * @param list
     * @throws Exception
     */
    @Override
    public void persistAlertNotificationTypes(List<AlertNotificationEntity> list) throws Exception {
        IEagleServiceClient client = new EagleServiceClientImpl(connector);
        GenericServiceAPIResponseEntity<String> response = client.create(list);
        client.close();
        if( !response.isSuccess() )
            throw new Exception(" Alert Notification Entities creation failed ");
    }
}
