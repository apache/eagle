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
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.eagle.alert.entity.AlertAPIEntity;
import org.apache.eagle.alert.entity.AlertDefinitionAPIEntity;
import org.apache.eagle.notification.base.NotificationStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Plugin to persist alerts to Eagle Storage
 */
public class AlertEagleStorePlugin implements NotificationPlugin {
    private static final Logger LOG = LoggerFactory.getLogger(AlertEagleStorePlugin.class);
    private NotificationStatus status;
    private AlertEagleStorePersister persist;

    @Override
    public void init(Config config, List<AlertDefinitionAPIEntity> initAlertDefs) throws Exception {
        this.persist = new AlertEagleStorePersister(config);
        this.status = new NotificationStatus();
        LOG.info("initialized plugin for EagleStorePlugin");
    }

    @Override
    public void update(String policyId, Map<String,String> notificationConf , boolean isPolicyDelete ) throws Exception {
        if( isPolicyDelete ){
            LOG.info("Deleted policy ...");
            return;
        }
        LOG.info("created/updated plugin ...");
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
        LOG.info("write alert to eagle storage " + alertEntity);
        try{
            List<AlertAPIEntity> list = new ArrayList<AlertAPIEntity>();
            list.add(alertEntity);
            boolean result = persist.doPersist( list );
            if(result) {
                status.successful = true;
                status.errorMessage = "";
            }else{
                status.successful = false;
                status.errorMessage = "";
            }
        }catch (Exception ex ){
            status.successful = false;
            status.errorMessage = ex.getMessage();
            LOG.error("Fail writing alert entity to Eagle Store", ex);
        }
    }

    @Override
    public int hashCode(){
        return new HashCodeBuilder().append(getClass().getCanonicalName()).toHashCode();
    }

    @Override
    public boolean equals(Object o){
        if(o == this)
            return true;
        if(!(o instanceof AlertEagleStorePlugin))
            return false;
        return true;
    }
}