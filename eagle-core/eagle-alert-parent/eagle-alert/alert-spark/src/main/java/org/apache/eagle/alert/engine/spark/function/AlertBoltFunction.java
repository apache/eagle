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

package org.apache.eagle.alert.engine.spark.function;

import backtype.storm.metric.api.MultiCountMetric;
import org.apache.eagle.alert.coordination.model.AlertBoltSpec;
import org.apache.eagle.alert.engine.StreamContextImpl;
import org.apache.eagle.alert.engine.StreamSparkContextImpl;
import org.apache.eagle.alert.engine.coordinator.PolicyDefinition;
import org.apache.eagle.alert.engine.coordinator.StreamDefinition;
import org.apache.eagle.alert.engine.evaluator.CompositePolicyHandler;
import org.apache.eagle.alert.engine.evaluator.PolicyGroupEvaluator;
import org.apache.eagle.alert.engine.evaluator.PolicyStreamHandler;
import org.apache.eagle.alert.engine.evaluator.impl.AlertBoltOutputCollectorSparkWrapper;
import org.apache.eagle.alert.engine.evaluator.impl.PolicyGroupEvaluatorImpl;
import org.apache.eagle.alert.engine.model.AlertStreamEvent;
import org.apache.eagle.alert.engine.model.PartitionedEvent;
import org.apache.eagle.alert.engine.runner.MapComparator;
import org.apache.eagle.alert.engine.spark.model.PolicyState;
import org.apache.eagle.alert.engine.spark.model.SiddhiState;
import org.apache.eagle.alert.engine.utils.Constants;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

public class AlertBoltFunction implements PairFlatMapFunction<Iterator<Tuple2<Integer, PartitionedEvent>>, String, AlertStreamEvent> {

    private static final Logger LOG = LoggerFactory.getLogger(AlertBoltFunction.class);
    private AtomicReference<Map<String, StreamDefinition>> sdsRef;
    private AtomicReference<AlertBoltSpec> alertBoltSpecRef;
    private PolicyState policyState;
    private SiddhiState siddhiState;

    public AlertBoltFunction(AtomicReference<Map<String, StreamDefinition>> sdsRef, AtomicReference<AlertBoltSpec> alertBoltSpecRef, PolicyState policyState, SiddhiState siddhiState) {
        this.sdsRef = sdsRef;
        this.alertBoltSpecRef = alertBoltSpecRef;
        this.policyState = policyState;
        this.siddhiState = siddhiState;
    }

    @Override
    public Iterator<Tuple2<String, AlertStreamEvent>> call(Iterator<Tuple2<Integer, PartitionedEvent>> tuple2Iterator) throws Exception {

        PolicyGroupEvaluatorImpl policyGroupEvaluator = null;
        AlertBoltOutputCollectorSparkWrapper alertOutputCollector = null;
        AlertBoltSpec spec;
        Map<String, StreamDefinition> sds;
        int partitionNum = Constants.UNKNOW_PARTITION;
        String boltId = Constants.ALERTBOLTNAME_PREFIX + partitionNum;
        while (tuple2Iterator.hasNext()) {
            Tuple2<Integer, PartitionedEvent> tuple2 = tuple2Iterator.next();
            if (partitionNum == Constants.UNKNOW_PARTITION) {
                partitionNum = tuple2._1;
            }
            if (policyGroupEvaluator == null) {
                spec = alertBoltSpecRef.get();
                sds = sdsRef.get();
                boltId = Constants.ALERTBOLTNAME_PREFIX + partitionNum;
                policyGroupEvaluator = new PolicyGroupEvaluatorImpl(boltId + "-evaluator_stage1");
                alertOutputCollector = new AlertBoltOutputCollectorSparkWrapper();
                //TODO StreamContext need to be more abstract
                Map<String, PolicyDefinition> policyDefinitionMap = policyState.getPolicyDefinitionByBoltId(boltId);
                Map<String, CompositePolicyHandler> policyStreamHandlerMap = policyState.getPolicyStreamHandlerByBoltId(boltId);
                byte[] siddhiSnapShot = siddhiState.getSiddhiSnapShotByBoltIdAndPartitionNum(boltId, partitionNum);
                policyGroupEvaluator.init(new StreamSparkContextImpl(null), alertOutputCollector, policyDefinitionMap, policyStreamHandlerMap, siddhiSnapShot);
                onAlertBoltSpecChange(boltId, spec, sds, policyGroupEvaluator, policyState);
            }
            PartitionedEvent event = tuple2._2;
            policyGroupEvaluator.nextEvent(event);
        }

        cleanup(policyGroupEvaluator, alertOutputCollector, boltId, partitionNum);
        if (alertOutputCollector == null) {
            return Collections.emptyIterator();
        }
        return alertOutputCollector.emitResult().iterator();
    }

    private void onAlertBoltSpecChange(String boltId, AlertBoltSpec spec, Map<String, StreamDefinition> sds, PolicyGroupEvaluatorImpl policyGroupEvaluator, PolicyState policyState) {
        List<PolicyDefinition> newPolicies = spec.getBoltPoliciesMap().get(boltId);
        LOG.debug("newPolicies {}", newPolicies);
        if (newPolicies == null) {
            LOG.info("no new policy with AlertBoltSpec {} for this bolt {}", spec, boltId);
            return;
        }

        Map<String, PolicyDefinition> newPoliciesMap = new HashMap<>();
        newPolicies.forEach(p -> newPoliciesMap.put(p.getName(), p));
        LOG.debug("newPoliciesMap {}", newPoliciesMap);
        MapComparator<String, PolicyDefinition> comparator = new MapComparator<>(newPoliciesMap, policyState.getCachedPolicyByBoltId(boltId));
        comparator.compare();
        LOG.debug("cachedPolicies {}", policyState.getCachedPolicyByBoltId(boltId));
        LOG.debug("getAdded {}", comparator.getAdded());
        LOG.debug("getRemoved {}", comparator.getRemoved());
        LOG.debug("getModified {}", comparator.getModified());
        policyGroupEvaluator.onPolicyChange(comparator.getAdded(), comparator.getRemoved(), comparator.getModified(), sds);

        policyState.store(boltId, newPoliciesMap, policyGroupEvaluator.getPolicyDefinitionMap(), policyGroupEvaluator.getPolicyStreamHandlerMap());
    }

    public void cleanup(PolicyGroupEvaluatorImpl policyGroupEvaluator, AlertBoltOutputCollectorSparkWrapper alertOutputCollector, String boltId, int partitionNum) {
        if (policyGroupEvaluator != null) {
            siddhiState.store(policyGroupEvaluator.closeAndSnapShot(), boltId, partitionNum);
        }
        if (alertOutputCollector != null) {
            alertOutputCollector.flush();
            alertOutputCollector.close();
        }
    }
}
