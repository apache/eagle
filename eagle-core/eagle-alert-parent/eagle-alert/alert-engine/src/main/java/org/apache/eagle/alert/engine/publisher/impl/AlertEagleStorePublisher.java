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

package org.apache.eagle.alert.engine.publisher.impl;

import java.util.Arrays;
import java.util.Map;

import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.eagle.alert.engine.coordinator.Publishment;
import org.apache.eagle.alert.engine.model.AlertStreamEvent;
import org.apache.eagle.alert.engine.publisher.AlertDeduplicator;
import org.apache.eagle.alert.engine.publisher.AlertPublishPlugin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.typesafe.config.Config;

/**
 * Plugin to persist alerts to Eagle Storage
 */
public class AlertEagleStorePublisher implements AlertPublishPlugin {

    private static final Logger LOG = LoggerFactory.getLogger(AlertEagleStorePublisher.class);
    private PublishStatus status;
    private AlertEagleStorePersister persist;
    private AlertDeduplicator deduplicator;

    @Override
    public void init(Config config, Publishment publishment) throws Exception {
        this.persist = new AlertEagleStorePersister(config);
        deduplicator = new DefaultDeduplicator(publishment.getDedupIntervalMin());
        LOG.info("initialized plugin for EagleStorePlugin");
    }

    @Override
    public void update(String dedupIntervalMin, Map<String, String> pluginProperties) {
        deduplicator.setDedupIntervalMin(dedupIntervalMin);
    }

    @Override
    public PublishStatus getStatus() {
        return this.status;
    }

    @Override
    public AlertStreamEvent dedup(AlertStreamEvent event) {
        return deduplicator.dedup(event);
    }

    /**
     * Persist AlertEntity to alert_details table
     * @param event
     */
    @Override
    public void onAlert(AlertStreamEvent event) {
        LOG.info("write alert to eagle storage " + event);
        event = dedup(event);
        if(event == null) {
            return;
        }
        PublishStatus status = new PublishStatus();
        try{
            boolean result = persist.doPersist(Arrays.asList(event));
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
        this.status = status;
    }

    @Override
    public void close() {

    }

    @Override
    public int hashCode(){
        return new HashCodeBuilder().append(getClass().getCanonicalName()).toHashCode();
    }

    @Override
    public boolean equals(Object o){
        if(o == this)
            return true;
        if(!(o instanceof AlertEagleStorePublisher))
            return false;
        return true;
    }
}