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
import org.apache.eagle.alert.entity.AlertAPIEntity;
import org.apache.eagle.common.config.EagleConfigFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.List;

/**
 * Responsible to persist Alerts to Eagle Storage
 */
@Resource(name = "Eagle Store" , description = "Persist Alert Entity to Eagle Store")
public class PersistToEagleStore implements  NotificationPlugin {

    private static final Logger LOG = LoggerFactory.getLogger(PersistToEagleStore.class);
    private NotificationStatus status;

    private Config config;
    private EagleAlertPersist persist;

    @Override
    public void _init() throws Exception {
        config = EagleConfigFactory.load().getConfig();
        this.persist = new EagleAlertPersist(config.getString("eagleProps.eagleService.host"), /*config.getInt("eagleProps.eagleService.port")*/ 8080,
                                             config.getString("eagleProps.eagleService.username"), config.getString("eagleProps.eagleService.password"));
    }

    @Override
    public NotificationStatus getStatus() {
        return this.status;
    }

    @Override
    public void onAlert(AlertAPIEntity alertEntity) {
        try{
            status = new NotificationStatus();
            List<AlertAPIEntity> list = new ArrayList<AlertAPIEntity>();
            list.add(alertEntity);
            persist.doPersist( list );
            status.setNotificationSuccess(true);
        }catch (Exception ex ){
            status.setMessage(ex.getMessage());
            LOG.error(" Exception when Posting Alert Entity to Eagle Service Topic. Reason : "+ex.getMessage());
        }
    }
}
