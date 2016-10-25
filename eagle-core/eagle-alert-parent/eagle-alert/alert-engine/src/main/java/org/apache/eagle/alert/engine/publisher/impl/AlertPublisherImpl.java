/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.eagle.alert.engine.publisher.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.eagle.alert.engine.coordinator.PublishPartition;
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
    private static final Logger LOG = LoggerFactory.getLogger(AlertPublisherImpl.class);

    private final String name;

    private volatile Map<PublishPartition, AlertPublishPlugin> publishPluginMapping = new ConcurrentHashMap<>(1);
    private Config config;
    private Map conf;

    public AlertPublisherImpl(String name) {
        this.name = name;
    }

    public AlertPublisherImpl(String name, List<Publishment> publishments) {
        this.name = name;
        for (Publishment publishment : publishments) {
            AlertPublishPlugin plugin = AlertPublishPluginsFactory.createNotificationPlugin(publishment, config, conf);
            if (plugin != null) {
                publishPluginMapping.put(publishment.getName(), plugin);
                addPublishmentPolicies(policyPublishPluginMapping, publishment.getPolicyIds(), publishment.getName());
            } else {
                LOG.error("Initialized alertPublisher {} failed due to invalid format", publishment);
            }
        }
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
    public void nextEvent(PublishPartition partition, AlertStreamEvent event) {
        if (LOG.isDebugEnabled()) {
            LOG.debug(event.toString());
        }
        notifyAlert(partition, event);
    }

    private void notifyAlert(PublishPartition partition, AlertStreamEvent event) {
        // remove the column values for publish plugin match
        partition.getColumnValues().clear();
        if (!publishPluginMapping.containsKey(partition)) {
            LOG.warn("PublishPartition {} is not found in publish plugin map", partition);
            return;
        }
        AlertPublishPlugin plugin = publishPluginMapping.get(partition);
        if (plugin == null) {
            LOG.warn("PublishPartition {} has problems while initializing publish plugin", partition);
            return;
        }
        event.ensureAlertId();
        try {
            LOG.debug("Execute alert publisher {}", plugin.getClass().getCanonicalName());
            plugin.onAlert(event);
        } catch (Exception ex) {
            LOG.error("Fail invoking publisher's onAlert, continue ", ex);
        }
    }

    @Override
    public void close() {
        publishPluginMapping.values().forEach(plugin -> plugin.close());
    }

    @Override
    public synchronized void onPublishChange(List<Publishment> added,
                                             List<Publishment> removed,
                                             List<Publishment> afterModified,
                                             List<Publishment> beforeModified) {
        if (added == null) {
            added = new ArrayList<>();
        }
        if (removed == null) {
            removed = new ArrayList<>();
        }
        if (afterModified == null) {
            afterModified = new ArrayList<>();
        }
        if (beforeModified == null) {
            beforeModified = new ArrayList<>();
        }

        if (afterModified.size() != beforeModified.size()) {
            LOG.warn("beforeModified size != afterModified size");
            return;
        }

        // copy and swap to avoid concurrency issue
        Map<PublishPartition, AlertPublishPlugin> newPublishMap = new HashMap<>(publishPluginMapping);

        // added
        for (Publishment publishment : added) {
            LOG.debug("OnPublishmentChange : add publishment : {} ", publishment);

            AlertPublishPlugin plugin = AlertPublishPluginsFactory.createNotificationPlugin(publishment, config, conf);
            if (plugin != null) {
                for (PublishPartition p : getPublishPartitions(publishment)) {
                    newPublishMap.put(p, plugin);
                }
            } else {
                LOG.error("OnPublishChange alertPublisher {} failed due to invalid format", publishment);
            }
        }
        //removed
        List<AlertPublishPlugin> toBeClosed = new ArrayList<>();
        for (Publishment publishment : removed) {
            AlertPublishPlugin plugin = null;
            for (PublishPartition p : getPublishPartitions(publishment)) {
                if (plugin == null) {
                    plugin = newPublishMap.remove(p);
                } else {
                    newPublishMap.remove(p);
                }
            }
            if (plugin != null) {
                toBeClosed.add(plugin);
            }
        }
        // updated
        for (Publishment publishment : afterModified) {
            // for updated publishment, need to init them too
            AlertPublishPlugin newPlugin = AlertPublishPluginsFactory.createNotificationPlugin(publishment, config, conf);
            if (newPlugin != null) {
                AlertPublishPlugin plugin = null;
                for (PublishPartition p : getPublishPartitions(publishment)) {
                    if (plugin == null) {
                        plugin = newPublishMap.get(p);
                    }
                    newPublishMap.put(p, newPlugin);
                }
                if (plugin != null) {
                    toBeClosed.add(plugin);
                }
            } else {
                LOG.error("OnPublishChange alertPublisher {} failed due to invalid format", publishment);
            }
        }

        // now do the swap
        publishPluginMapping = newPublishMap;

        // safely close : it depend on plugin to check if want to wait all data to be flushed.
        closePlugins(toBeClosed);
    }

    private Set<PublishPartition> getPublishPartitions(Publishment publish) {
        List<String> streamIds = new ArrayList<>();
        // add the publish to the bolt
        if (publish.getStreamIds() == null || publish.getStreamIds().size() <= 0) {
            streamIds.add(Publishment.STREAM_NAME_DEFAULT);
        } else {
            streamIds.addAll(publish.getStreamIds());
        }
        Set<PublishPartition> publishPartitions = new HashSet<>();
        for (String streamId : streamIds) {
            for (String policyId : publish.getPolicyIds()) {
                publishPartitions.add(new PublishPartition(streamId, policyId, publish.getName(), publish.getPartitionColumns()));
            }
        }
        return publishPartitions;
    }

    private void closePlugins(List<AlertPublishPlugin> toBeClosed) {
        for (AlertPublishPlugin p : toBeClosed) {
            try {
                p.close();
            } catch (Exception e) {
                LOG.error(String.format("Error when close publish plugin {}!", p.getClass().getCanonicalName()), e);
            }
        }
    }

}
