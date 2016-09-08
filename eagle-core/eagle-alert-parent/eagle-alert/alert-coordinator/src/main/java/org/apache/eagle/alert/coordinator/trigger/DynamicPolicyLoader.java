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
package org.apache.eagle.alert.coordinator.trigger;

import org.apache.eagle.alert.engine.coordinator.PolicyDefinition;
import org.apache.eagle.alert.service.IMetadataServiceClient;
import com.google.common.base.Stopwatch;
import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Poll policy change and notify listeners.
 */
public class DynamicPolicyLoader implements Runnable {
    private static Logger LOG = LoggerFactory.getLogger(DynamicPolicyLoader.class);

    private IMetadataServiceClient client;
    // initial cachedPolicies should be empty
    private Map<String, PolicyDefinition> cachedPolicies = new HashMap<>();
    private List<PolicyChangeListener> listeners = new ArrayList<>();

    public DynamicPolicyLoader(IMetadataServiceClient client) {
        this.client = client;
    }

    public synchronized void addPolicyChangeListener(PolicyChangeListener listener) {
        listeners.add(listener);
    }

    /**
     * When it is run at the first time, due to cachedPolicies being empty, all existing policies are expected
     * to be addedPolicies.
     */
    @SuppressWarnings("unchecked")
    @Override
    public void run() {
        // we should catch every exception to avoid zombile thread
        try {
            final Stopwatch watch = Stopwatch.createStarted();
            LOG.info("policies loader start.");
            List<PolicyDefinition> current = client.listPolicies();
            Map<String, PolicyDefinition> currPolicies = new HashMap<>();
            current.forEach(pe -> currPolicies.put(pe.getName(), pe));

            Collection<String> addedPolicies = CollectionUtils.subtract(currPolicies.keySet(), cachedPolicies.keySet());
            Collection<String> removedPolicies = CollectionUtils.subtract(cachedPolicies.keySet(), currPolicies.keySet());
            Collection<String> potentiallyModifiedPolicies = CollectionUtils.intersection(currPolicies.keySet(), cachedPolicies.keySet());

            List<String> reallyModifiedPolicies = new ArrayList<>();
            for (String updatedPolicy : potentiallyModifiedPolicies) {
                if (!currPolicies.get(updatedPolicy).equals(cachedPolicies.get(updatedPolicy))) {
                    reallyModifiedPolicies.add(updatedPolicy);
                }
            }

            boolean policyChanged = false;
            if (addedPolicies.size() != 0
                || removedPolicies.size() != 0
                || reallyModifiedPolicies.size() != 0) {
                policyChanged = true;
            }

            if (!policyChanged) {
                LOG.info("policy is not changed since last run");
                return;
            }
            synchronized (this) {
                for (PolicyChangeListener listener : listeners) {
                    listener.onPolicyChange(current, addedPolicies, removedPolicies, reallyModifiedPolicies);
                }
            }

            watch.stop();
            LOG.info("policies loader completed. used time milliseconds: {}", watch.elapsed(TimeUnit.MILLISECONDS));
            // reset cached policies
            cachedPolicies = currPolicies;
        } catch (Throwable t) {
            LOG.error("error loading policy, but continue to run", t);
        }
    }
}
