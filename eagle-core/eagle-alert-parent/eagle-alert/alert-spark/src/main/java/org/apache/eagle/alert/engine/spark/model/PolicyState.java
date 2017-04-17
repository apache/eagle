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

import kafka.message.MessageAndMetadata;
import org.apache.eagle.alert.engine.coordinator.PolicyDefinition;
import org.apache.eagle.alert.engine.evaluator.CompositePolicyHandler;
import org.apache.eagle.alert.engine.spark.accumulator.MapToMapAccum;
import org.apache.spark.Accumulator;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

public class PolicyState implements Serializable {
    private static final Logger LOG = LoggerFactory.getLogger(PolicyState.class);
    private static final long serialVersionUID = 5566434575066996563L;

    private AtomicReference<Map<String, Map<String, PolicyDefinition>>> cachedPoliciesRef = new AtomicReference<>();

    private Accumulator<Map<String, Map<String, PolicyDefinition>>> cachedPolicies;

    private AtomicReference<Map<String, Map<String, PolicyDefinition>>> policyDefinitionRef = new AtomicReference<>();

    private Accumulator<Map<String, Map<String, PolicyDefinition>>> policyDefinition;

    private AtomicReference<Map<String, Map<String, CompositePolicyHandler>>> policyStreamHandlerRef = new AtomicReference<>();

    private Accumulator<Map<String, Map<String, CompositePolicyHandler>>> policyStreamHandler;

    public PolicyState() {
    }

    public void initailPolicyState(JavaRDD<MessageAndMetadata<String, String>> rdd) {
        Accumulator<Map<String, Map<String, PolicyDefinition>>> cachedPolicies =
            StateInstance.getInstance(new JavaSparkContext(rdd.context()), "policyAccum", new MapToMapAccum());
        Accumulator<Map<String, Map<String, PolicyDefinition>>> policyDefinition =
            StateInstance.getInstance(new JavaSparkContext(rdd.context()), "policyDefinitionAccum", new MapToMapAccum());
        Accumulator<Map<String, Map<String, CompositePolicyHandler>>> policyStreamHandler =
            StateInstance.getInstance(new JavaSparkContext(rdd.context()), "policyStreamHandlerAccum", new MapToMapAccum());
        this.cachedPolicies = cachedPolicies;
        this.policyDefinition = policyDefinition;
        this.policyStreamHandler = policyStreamHandler;
    }


    public void recover(JavaRDD<MessageAndMetadata<String, String>> rdd) {
        initailPolicyState(rdd);
        cachedPoliciesRef.set(cachedPolicies.value());
        policyDefinitionRef.set(policyDefinition.value());
        policyStreamHandlerRef.set(policyStreamHandler.value());
        LOG.debug("---------cachedPoliciesRef----------" + cachedPoliciesRef.get());
        LOG.debug("---------policyDefinitionRef----------" + policyDefinitionRef.get());
        LOG.debug("---------policyStreamHandlerRef----------" + policyStreamHandlerRef.get());
    }

    public void store(String boltId, Map<String, PolicyDefinition> cachedPoliciesMap, Map<String, PolicyDefinition> newPolicyDefinition, Map<String, CompositePolicyHandler> newPolicyStreamHandler) {
        Map<String, Map<String, PolicyDefinition>> cachedPolicy = new HashMap<>();
        cachedPolicy.put(boltId, cachedPoliciesMap);
        LOG.debug("---------store---cachedPoliciesMap----------" + cachedPoliciesMap);
        cachedPolicies.add(cachedPolicy);

        Map<String, Map<String, PolicyDefinition>> policyDefinitionMap = new HashMap<>();
        policyDefinitionMap.put(boltId, newPolicyDefinition);
        policyDefinition.add(policyDefinitionMap);

        Map<String, Map<String, CompositePolicyHandler>> policyStreamHandlerMap = new HashMap<>();
        policyStreamHandlerMap.put(boltId, newPolicyStreamHandler);
        policyStreamHandler.add(policyStreamHandlerMap);

    }

    public Map<String, PolicyDefinition> getCachedPolicyByBoltId(String boltId) {
        Map<String, Map<String, PolicyDefinition>> boltIdToPolicy = cachedPoliciesRef.get();
        LOG.debug("---PolicyState----getPolicyByBoltId----------" + (boltIdToPolicy));
        Map<String, PolicyDefinition> boltIdToPolicykMap = boltIdToPolicy.get(boltId);
        if (boltIdToPolicykMap == null) {
            boltIdToPolicykMap = new HashMap<>();
        }
        return boltIdToPolicykMap;
    }

    public Map<String, PolicyDefinition> getPolicyDefinitionByBoltId(String boltId) {
        Map<String, Map<String, PolicyDefinition>> boltIdToPolicy = policyDefinitionRef.get();
        LOG.debug("---PolicyState----getPolicyDefinitionByBoltId----------" + (boltIdToPolicy));
        Map<String, PolicyDefinition> boltIdToPolicyMap = boltIdToPolicy.get(boltId);
        if (boltIdToPolicyMap == null) {
            boltIdToPolicyMap = new HashMap<>();
        }
        return boltIdToPolicyMap;
    }

    public Map<String, CompositePolicyHandler> getPolicyStreamHandlerByBoltId(String boltId) {
        Map<String, Map<String, CompositePolicyHandler>> boltIdToPolicyStreamHandler = policyStreamHandlerRef.get();
        LOG.debug("---PolicyState----getPolicyStreamHandlerByBoltId----------" + (boltIdToPolicyStreamHandler));
        Map<String, CompositePolicyHandler> boltIdToPolicyStreamHandlerMap = boltIdToPolicyStreamHandler.get(boltId);
        if (boltIdToPolicyStreamHandlerMap == null) {
            boltIdToPolicyStreamHandlerMap = new HashMap<>();
        }
        return boltIdToPolicyStreamHandlerMap;
    }
}
