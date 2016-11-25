/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.eagle.alert.app;

import com.google.common.base.Preconditions;
import com.typesafe.config.Config;
import org.apache.eagle.alert.engine.coordinator.Publishment;
import org.apache.eagle.alert.engine.model.AlertStreamEvent;
import org.apache.eagle.alert.engine.publisher.impl.AbstractPublishPlugin;
import org.apache.eagle.alert.utils.AlertConstants;
import org.apache.eagle.metadata.model.AlertEntity;
import org.apache.eagle.service.client.IEagleServiceClient;
import org.apache.eagle.service.client.impl.EagleServiceClientImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.eagle.alert.engine.model.AlertPublishEvent.*;

public class AlertEagleStorePlugin extends AbstractPublishPlugin {
    private static final Logger LOG = LoggerFactory.getLogger(AlertEagleStorePlugin.class);
    private IEagleServiceClient client;

    @Override
    public void init(Config config, Publishment publishment, Map conf) throws Exception {
        super.init(config, publishment, conf);
        client = new EagleServiceClientImpl(config.getString("service.host"), config.getInt("service.port"),
                config.getString("service.username"), config.getString("service.password"));
    }

    @Override
    public void close() {
        try {
            client.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void onAlert(AlertStreamEvent event) throws Exception {
        List<AlertStreamEvent> eventList = this.dedup(event);
        if (eventList == null || eventList.isEmpty()) {
            return;
        }
        List<AlertEntity> alertEvents = new ArrayList<>();
        for (AlertStreamEvent e : eventList) {
            alertEvents.add(convertAlertEvent(e));
        }
        client.create(alertEvents, AlertConstants.ALERT_SERVICE_ENDPOINT_NAME);
    }

    @Override
    protected Logger getLogger() {
        return LOG;
    }

    public AlertEntity convertAlertEvent(AlertStreamEvent event) {
        Preconditions.checkNotNull(event.getAlertId(), "alertId is not initialized before being published: " + event.toString());
        AlertEntity alertEvent = new AlertEntity();
        Map<String, String> tags = new HashMap<>();
        tags.put(POLICY_ID_KEY, event.getPolicyId());
        tags.put(ALERT_ID_KEY, event.getAlertId());
        if (event.getExtraData() != null && !event.getExtraData().isEmpty()) {
            tags.put(SITE_ID_KEY, event.getExtraData().get(SITE_ID_KEY).toString());
            alertEvent.setPolicyValue(event.getExtraData().get(POLICY_VALUE_KEY).toString());
            alertEvent.setAppIds((List<String>) event.getExtraData().get(APP_IDS_KEY));
        }
        alertEvent.setTimestamp(event.getCreatedTime());
        alertEvent.setAlertData(event.getDataMap());
        alertEvent.setTags(tags);
        return alertEvent;
    }
}
