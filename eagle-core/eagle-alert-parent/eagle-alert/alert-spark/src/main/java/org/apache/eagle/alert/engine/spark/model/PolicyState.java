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

package org.apache.eagle.alert.engine.spark.model;

import org.apache.eagle.alert.engine.coordinator.PolicyDefinition;
import org.apache.eagle.alert.engine.evaluator.PolicyStreamHandler;
import org.apache.eagle.alert.engine.spark.accumulator.MapAccum;
import org.apache.spark.Accumulator;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

public class PolicyState implements Serializable {
    private static final Logger LOG = LoggerFactory.getLogger(PolicyState.class);

    private AtomicReference<Map<String, Map<String, PolicyDefinition>>> cachedPoliciesRef = new AtomicReference<>();
    private Accumulator<Map<String, Map<String, PolicyDefinition>>> cachedPolicies;

    private AtomicReference<Map<String, Map<String, PolicyDefinition>>> policyDefinitionRef = new AtomicReference<>();

    private Accumulator<Map<String, Map<String, PolicyDefinition>>> policyDefinition;

    private AtomicReference<Map<String, Map<String, PolicyStreamHandler>>> policyStreamHandlerRef = new AtomicReference<>();

    private Accumulator<Map<String, Map<String, PolicyStreamHandler>>> policyStreamHandler;

    public PolicyState(JavaStreamingContext jssc){
        Accumulator<Map<String, Map<String, PolicyDefinition>>> cachedPolicies = jssc.sparkContext().accumulator(new HashMap<>(), "policyAccum", new MapAccum());
        Accumulator<Map<String, Map<String, PolicyDefinition>>> policyDefinition = jssc.sparkContext().accumulator(new HashMap<>(), "policyDefinitionAccum", new MapAccum());
        Accumulator<Map<String, Map<String, PolicyStreamHandler>>> policyStreamHandler = jssc.sparkContext().accumulator(new HashMap<>(), "policyStreamHandlerAccum", new MapAccum());
        this.cachedPolicies = cachedPolicies;
        this.policyDefinition = policyDefinition;
        this.policyStreamHandler = policyStreamHandler;
    }

    public PolicyState(Accumulator<Map<String, Map<String, PolicyDefinition>>> cachedPolicies, Accumulator<Map<String, Map<String, PolicyDefinition>>> policyDefinition, Accumulator<Map<String, Map<String, PolicyStreamHandler>>> policyStreamHandler) {
        this.cachedPolicies = cachedPolicies;
        this.policyDefinition = policyDefinition;
        this.policyStreamHandler = policyStreamHandler;
    }

    public void recover() {
        cachedPoliciesRef.set(cachedPolicies.value());
        policyDefinitionRef.set(policyDefinition.value());
        policyStreamHandlerRef.set(policyStreamHandler.value());
        LOG.info("---------routeSpecMapRef----------" + cachedPoliciesRef.get());
        LOG.info("---------policyDefinitionRef----------" + policyDefinitionRef.get());
        LOG.info("---------policyStreamHandlerRef----------" + policyStreamHandlerRef.get());
    }

    public void store(String boltId, Map<String, PolicyDefinition> cachedPoliciesMap, Map<String, PolicyDefinition> newPolicyDefinition, Map<String, PolicyStreamHandler> newPolicyStreamHandler) {
        Map<String, Map<String, PolicyDefinition>> cachedPolicy = new HashMap<>();
        cachedPolicy.put(boltId, cachedPoliciesMap);
        cachedPolicies.add(cachedPolicy);

        Map<String, Map<String, PolicyDefinition>> policyDefinitionMap = new HashMap<>();
        policyDefinitionMap.put(boltId, newPolicyDefinition);
        policyDefinition.add(policyDefinitionMap);

        Map<String, Map<String, PolicyStreamHandler>> policyStreamHandlerMap = new HashMap<>();
        policyStreamHandlerMap.put(boltId, newPolicyStreamHandler);
        policyStreamHandler.add(policyStreamHandlerMap);

    }

    public Map<String, PolicyDefinition> getCachedPolicyByBoltId(String boltId) {
        Map<String, Map<String, PolicyDefinition>> boltIdToPolicy = cachedPoliciesRef.get();
        LOG.info("---PolicyState----getPolicyByBoltId----------" + (boltIdToPolicy));
        Map<String, PolicyDefinition> boltIdToPolicykMap = boltIdToPolicy.get(boltId);
        if (boltIdToPolicykMap == null) {
            boltIdToPolicykMap = new HashMap<>();
        }
        return boltIdToPolicykMap;
    }

    public Map<String, PolicyDefinition> getPolicyDefinitionByBoltId(String boltId) {
        Map<String, Map<String, PolicyDefinition>> boltIdToPolicy = policyDefinitionRef.get();
        LOG.info("---PolicyState----getPolicyDefinitionByBoltId----------" + (boltIdToPolicy));
        Map<String, PolicyDefinition> boltIdToPolicyMap = boltIdToPolicy.get(boltId);
        if (boltIdToPolicyMap == null) {
            boltIdToPolicyMap = new HashMap<>();
        }
        return boltIdToPolicyMap;
    }

    public Map<String, PolicyStreamHandler> getPolicyStreamHandlerByBoltId(String boltId) {
        Map<String, Map<String, PolicyStreamHandler>> boltIdToPolicyStreamHandler = policyStreamHandlerRef.get();
        LOG.info("---PolicyState----getPolicyStreamHandlerByBoltId----------" + (boltIdToPolicyStreamHandler));
        Map<String, PolicyStreamHandler> boltIdToPolicyStreamHandlerMap = boltIdToPolicyStreamHandler.get(boltId);
        if (boltIdToPolicyStreamHandlerMap == null) {
            boltIdToPolicyStreamHandlerMap = new HashMap<>();
        }
        return boltIdToPolicyStreamHandlerMap;
    }
}
