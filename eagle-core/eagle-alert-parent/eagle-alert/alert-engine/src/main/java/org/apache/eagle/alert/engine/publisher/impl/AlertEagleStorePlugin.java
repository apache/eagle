/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *  <p/>
 *  http://www.apache.org/licenses/LICENSE-2.0
 *  <p/>
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

package org.apache.eagle.alert.engine.publisher.impl;

import com.typesafe.config.Config;
import org.apache.eagle.alert.engine.coordinator.Publishment;
import org.apache.eagle.alert.engine.coordinator.PublishmentType;
import org.apache.eagle.alert.engine.model.AlertPublishEvent;
import org.apache.eagle.alert.engine.model.AlertStreamEvent;
import org.apache.eagle.alert.engine.publisher.AlertPublishPluginProvider;
import org.apache.eagle.alert.service.IMetadataServiceClient;
import org.apache.eagle.alert.service.MetadataServiceClientImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class AlertEagleStorePlugin extends AbstractPublishPlugin implements AlertPublishPluginProvider {

    private static Logger LOG = LoggerFactory.getLogger(AlertEagleStorePlugin.class);
    private transient IMetadataServiceClient client;

    @Override
    public void init(Config config, Publishment publishment, Map conf) throws Exception {
        super.init(config, publishment, conf);
        client = new MetadataServiceClientImpl(config);
    }

    @Override
    public void close() {
        try {
            client.close();
        } catch (IOException e) {
            LOG.error(e.getMessage(), e);
        }
    }

    @Override
    public void onAlert(AlertStreamEvent event) throws Exception {
        List<AlertStreamEvent> eventList = this.dedup(event);
        if (eventList == null || eventList.isEmpty()) {
            return;
        }
        List<AlertPublishEvent> alertEvents = eventList.stream().map(
            AlertPublishEvent::createAlertPublishEvent
        ).collect(Collectors.toList());
        client.addAlertPublishEvents(alertEvents);
    }

    @Override
    protected Logger getLogger() {
        return LOG;
    }

    @Override
    public PublishmentType getPluginType() {
        return new PublishmentType.Builder()
                .name("JDBCStorage")
                .type(getClass())
                .description("Publish alerts into eagle metadata store")
                .build();
    }
}