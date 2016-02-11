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
import org.apache.eagle.alert.entity.AlertAPIEntity;
import org.apache.eagle.alert.entity.AlertDefinitionAPIEntity;
import org.apache.eagle.notification.base.NotificationConstants;
import org.apache.eagle.notification.base.NotificationMetadata;
import org.apache.eagle.notification.base.NotificationStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Plugin to persist alerts to Eagle Storage
 */
public class AlertEagleStorePlugin implements NewNotificationPlugin {
    private static final Logger LOG = LoggerFactory.getLogger(AlertEagleStorePlugin.class);
    private NotificationStatus status;
    private AlertEagleStorePersister persist;

    @Override
    public NotificationMetadata getMetadata() {
        NotificationMetadata metadata = new NotificationMetadata();
        metadata.name = NotificationConstants.EAGLE_STORE;
        metadata.description = "Persist Alert Entity to Eagle Store";
        return metadata;
    }

    @Override
    public void init(Config config, List<AlertDefinitionAPIEntity> initAlertDefs) throws Exception {
        this.persist = new AlertEagleStorePersister(config);
        this.status = new NotificationStatus();
    }

    @Override
    public void update(Map<String,String> notificationConf , boolean isPolicyDelete ) throws Exception {
        if( isPolicyDelete ){
            LOG.info("Deleted policy ...");
        }
    }

    @Override
    public NotificationStatus getStatus() {
        return this.status;
    }

    /**
     * Persist AlertEntity to alert_details table
     * @param alertEntity
     */
    @Override
    public void onAlert(AlertAPIEntity alertEntity) {
        try{
            List<AlertAPIEntity> list = new ArrayList<AlertAPIEntity>();
            list.add(alertEntity);
            persist.doPersist( list );
            status.successful = true;
            status.errorMessage = "";
        }catch (Exception ex ){
            status.successful = false;
            status.errorMessage = ex.getMessage();
            LOG.error("Fail writing alert entity to Eagle Store", ex);
        }
    }
}