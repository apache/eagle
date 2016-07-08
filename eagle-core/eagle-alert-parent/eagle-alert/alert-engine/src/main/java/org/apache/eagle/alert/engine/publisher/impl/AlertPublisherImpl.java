/**
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

package org.apache.eagle.alert.engine.publisher.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.collections.ListUtils;
import org.apache.eagle.alert.engine.coordinator.Publishment;
import org.apache.eagle.alert.engine.model.AlertStreamEvent;
import org.apache.eagle.alert.engine.publisher.AlertPublishPlugin;
import org.apache.eagle.alert.engine.publisher.AlertPublisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.typesafe.config.Config;

@SuppressWarnings("rawtypes")
public class AlertPublisherImpl implements AlertPublisher {
    private static final long serialVersionUID = 4809983246198138865L;
    private final static Logger LOG = LoggerFactory.getLogger(AlertPublisherImpl.class);
    private final String name;

    private volatile Map<String, List<String>> policyPublishPluginMapping = new ConcurrentHashMap<>(1);
    private volatile Map<String, AlertPublishPlugin> publishPluginMapping = new ConcurrentHashMap<>(1);
    private Config config;
    private Map conf;

    public AlertPublisherImpl(String name) {
        this.name = name;
    }

    @Override
    public void init(Config config, Map conf) {
        this.config = config;
        this.conf = conf;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public void nextEvent(AlertStreamEvent event) {
        if(LOG.isDebugEnabled())
            LOG.debug(event.toString());
        notifyAlert(event);
    }

    private void notifyAlert(AlertStreamEvent event) {
        String policyId = event.getPolicyId();
        if(policyId == null || !policyPublishPluginMapping.containsKey(policyId)) {
            LOG.warn("Policy {} does NOT subscribe any publishments", policyId);
            return;
        }
        for(String id: policyPublishPluginMapping.get(policyId)) {
            AlertPublishPlugin plugin = publishPluginMapping.get(id);
            try {
                if(LOG.isDebugEnabled()) LOG.debug("Execute alert publisher " + plugin.getClass().getCanonicalName());
                plugin.onAlert(event);
            } catch (Exception ex) {
                LOG.error("Fail invoking publisher's onAlert, continue ", ex);
            }
        }
    }

    @Override
    public void close() {
        publishPluginMapping.values().forEach(plugin -> plugin.close());
    }

    @SuppressWarnings("unchecked")
    @Override
    public void onPublishChange(List<Publishment> added,
                                List<Publishment> removed,
                                List<Publishment> afterModified,
                                List<Publishment> beforeModified) {
        if (added == null) added = new ArrayList<>();
        if (removed == null) removed = new ArrayList<>();
        if (afterModified == null) afterModified = new ArrayList<>();
        if (beforeModified == null) beforeModified = new ArrayList<>();

        if (afterModified.size() != beforeModified.size()) {
            LOG.warn("beforeModified size != afterModified size");
            return;
        }

        for (Publishment publishment : added) {
            AlertPublishPlugin plugin = AlertPublishPluginsFactory.createNotificationPlugin(publishment, config, conf);
            if(plugin != null) {
                publishPluginMapping.put(publishment.getName(), plugin);
                onPolicyAdded(publishment.getPolicyIds(), publishment.getName());
            } else {
                LOG.error("Initialized alertPublisher {} failed due to invalid format", publishment);
            }
        }
        for (Publishment publishment : removed) {
            String pubName = publishment.getName();
            onPolicyDeleted(publishment.getPolicyIds(), pubName);
            publishPluginMapping.get(pubName).close();
            publishPluginMapping.remove(publishment.getName());
        }
        for (int i = 0; i < afterModified.size(); i++) {
            String pubName = afterModified.get(i).getName();
            List<String> newPolicies = afterModified.get(i).getPolicyIds();
            List<String> oldPolicies = beforeModified.get(i).getPolicyIds();

            if (! newPolicies.equals(oldPolicies)) {
                List<String> deletedPolicies = ListUtils.subtract(oldPolicies, newPolicies);
                onPolicyDeleted(deletedPolicies, pubName);
                List<String> addedPolicies = ListUtils.subtract(newPolicies, oldPolicies);
                onPolicyAdded(addedPolicies, pubName);
            }
            Publishment newPub = afterModified.get(i);
            publishPluginMapping.get(pubName).update(newPub.getDedupIntervalMin(), newPub.getProperties());
        }
    }

    private synchronized void onPolicyAdded(List<String> addedPolicyIds, String pubName) {
        if (addedPolicyIds == null || pubName == null) return;

        for (String policyId : addedPolicyIds) {
            if (policyPublishPluginMapping.get(policyId) == null) {
                policyPublishPluginMapping.put(policyId, new ArrayList<>());
            }
            List<String> publishIds = policyPublishPluginMapping.get(policyId);
            publishIds.add(pubName);
        }
    }

    private synchronized void onPolicyDeleted(List<String> deletedPolicyIds, String pubName) {
        if (deletedPolicyIds == null || pubName == null) return;

        for (String policyId : deletedPolicyIds) {
            List<String> publishIds = policyPublishPluginMapping.get(policyId);
            publishIds.remove(pubName);
        }
    }

}
