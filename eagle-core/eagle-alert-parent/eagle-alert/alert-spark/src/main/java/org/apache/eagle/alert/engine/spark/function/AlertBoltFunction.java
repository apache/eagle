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

import org.apache.commons.collections.CollectionUtils;
import org.apache.eagle.alert.coordination.model.AlertBoltSpec;
import org.apache.eagle.alert.engine.StreamContext;
import org.apache.eagle.alert.engine.StreamSparkContextImpl;
import org.apache.eagle.alert.engine.coordinator.PolicyDefinition;
import org.apache.eagle.alert.engine.coordinator.PublishPartition;
import org.apache.eagle.alert.engine.coordinator.StreamDefinition;
import org.apache.eagle.alert.engine.evaluator.CompositePolicyHandler;
import org.apache.eagle.alert.engine.evaluator.impl.AlertBoltOutputCollectorWrapper;
import org.apache.eagle.alert.engine.evaluator.impl.PolicyGroupEvaluatorImpl;
import org.apache.eagle.alert.engine.model.AlertStreamEvent;
import org.apache.eagle.alert.engine.model.PartitionedEvent;
import org.apache.eagle.alert.engine.router.impl.SparkOutputCollector;
import org.apache.eagle.alert.engine.runner.MapComparator;
import org.apache.eagle.alert.engine.spark.model.PolicyState;
import org.apache.eagle.alert.engine.spark.model.PublishState;
import org.apache.eagle.alert.engine.spark.model.SiddhiState;
import org.apache.eagle.alert.engine.utils.Constants;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

public class AlertBoltFunction implements PairFlatMapFunction<Iterator<Tuple2<Integer, Iterable<PartitionedEvent>>>, PublishPartition, AlertStreamEvent> {

    private static final Logger LOG = LoggerFactory.getLogger(AlertBoltFunction.class);
    private static final long serialVersionUID = -7876789777660951749L;
    private AtomicReference<Map<String, StreamDefinition>> sdsRef;
    private AtomicReference<AlertBoltSpec> alertBoltSpecRef;
    private PolicyState policyState;
    private SiddhiState siddhiState;
    private PublishState publishState;

    public AlertBoltFunction(AtomicReference<Map<String, StreamDefinition>> sdsRef,
                             AtomicReference<AlertBoltSpec> alertBoltSpecRef,
                             PolicyState policyState,
                             SiddhiState siddhiState,
                             PublishState publishState) {
        this.sdsRef = sdsRef;
        this.alertBoltSpecRef = alertBoltSpecRef;
        this.policyState = policyState;
        this.siddhiState = siddhiState;
        this.publishState = publishState;
    }


    @Override
    public Iterator<Tuple2<PublishPartition, AlertStreamEvent>> call(Iterator<Tuple2<Integer, Iterable<PartitionedEvent>>> tuple2Iterator) throws Exception {
        if (!tuple2Iterator.hasNext()) {
            return Collections.emptyIterator();
        }

        PolicyGroupEvaluatorImpl policyGroupEvaluator = null;
        AlertBoltOutputCollectorWrapper alertOutputCollector = null;
        AlertBoltSpec spec;
        StreamContext streamContext;
        Map<String, StreamDefinition> sds;
        Tuple2<Integer, Iterable<PartitionedEvent>> tuple2 = tuple2Iterator.next();
        Iterator<PartitionedEvent> events = tuple2._2.iterator();
        int partitionNum = tuple2._1;
        String boltId = Constants.ALERTBOLTNAME_PREFIX + partitionNum;
        while (events.hasNext()) {
            if (policyGroupEvaluator == null) {
                spec = alertBoltSpecRef.get();
                sds = sdsRef.get();
                boltId = Constants.ALERTBOLTNAME_PREFIX + partitionNum;
                policyGroupEvaluator = new PolicyGroupEvaluatorImpl(boltId + "-evaluator_stage1");
                Set<PublishPartition> cachedPublishPartitions = publishState.getCachedPublishPartitionsByBoltId(boltId);
                streamContext = new StreamSparkContextImpl(null);
                alertOutputCollector = new AlertBoltOutputCollectorWrapper(new SparkOutputCollector(), streamContext, cachedPublishPartitions);
                Map<String, PolicyDefinition> policyDefinitionMap = policyState.getPolicyDefinitionByBoltId(boltId);
                Map<String, CompositePolicyHandler> policyStreamHandlerMap = policyState.getPolicyStreamHandlerByBoltId(boltId);
                byte[] siddhiSnapShot = siddhiState.getSiddhiSnapShotByBoltIdAndPartitionNum(boltId, partitionNum);
                policyGroupEvaluator.init(streamContext, alertOutputCollector, policyDefinitionMap, policyStreamHandlerMap, siddhiSnapShot);
                onAlertBoltSpecChange(boltId, spec, sds, policyGroupEvaluator, alertOutputCollector, policyState, publishState);
            }
            PartitionedEvent event = events.next();
            policyGroupEvaluator.nextEvent(event);
        }

        cleanUpAndStoreSiddhiState(policyGroupEvaluator, alertOutputCollector, boltId, partitionNum);

        return alertOutputCollector.emitAll().iterator();
    }

    private void onAlertBoltSpecChange(String boltId,
                                       AlertBoltSpec spec,
                                       Map<String, StreamDefinition> sds,
                                       PolicyGroupEvaluatorImpl policyGroupEvaluator,
                                       AlertBoltOutputCollectorWrapper alertOutputCollector,
                                       PolicyState policyState, PublishState publishState) {
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
        LOG.debug("getAdded {}", comparator.getAdded());
        LOG.debug("getRemoved {}", comparator.getRemoved());
        LOG.debug("getModified {}", comparator.getModified());
        policyGroupEvaluator.onPolicyChange(comparator.getAdded(), comparator.getRemoved(), comparator.getModified(), sds);

        policyState.store(boltId, newPoliciesMap, policyGroupEvaluator.getPolicyDefinitionMap(), policyGroupEvaluator.getPolicyStreamHandlerMap());


        // update alert output collector
        Set<PublishPartition> newPublishPartitions = new HashSet<>();
        spec.getPublishPartitions().forEach(p -> {
            if (newPolicies.stream().filter(o -> o.getName().equals(p.getPolicyId())).count() > 0) {
                newPublishPartitions.add(p);
            }
        });

        Set<PublishPartition> cachedPublishPartitions = publishState.getCachedPublishPartitionsByBoltId(boltId);
        Collection<PublishPartition> addedPublishPartitions = CollectionUtils.subtract(newPublishPartitions, cachedPublishPartitions);
        Collection<PublishPartition> removedPublishPartitions = CollectionUtils.subtract(cachedPublishPartitions, newPublishPartitions);
        Collection<PublishPartition> modifiedPublishPartitions = CollectionUtils.intersection(newPublishPartitions, cachedPublishPartitions);

        LOG.debug("added PublishPartition " + addedPublishPartitions);
        LOG.debug("removed PublishPartition " + removedPublishPartitions);
        LOG.debug("modified PublishPartition " + modifiedPublishPartitions);

        alertOutputCollector.onAlertBoltSpecChange(addedPublishPartitions, removedPublishPartitions, modifiedPublishPartitions);

        publishState.storePublishPartitions(boltId, alertOutputCollector.getPublishPartitions());

    }

    public void cleanUpAndStoreSiddhiState(PolicyGroupEvaluatorImpl policyGroupEvaluator, AlertBoltOutputCollectorWrapper alertOutputCollector, String boltId, int partitionNum) {
        if (policyGroupEvaluator != null) {
            siddhiState.store(policyGroupEvaluator.closeAndSnapShot(), boltId, partitionNum);
        }
        if (alertOutputCollector != null) {
            alertOutputCollector.flush();
            alertOutputCollector.close();
        }
    }


}
