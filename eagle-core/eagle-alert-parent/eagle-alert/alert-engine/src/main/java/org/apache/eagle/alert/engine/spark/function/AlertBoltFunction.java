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

import org.apache.eagle.alert.coordination.model.AlertBoltSpec;
import org.apache.eagle.alert.engine.StreamContextImpl;
import org.apache.eagle.alert.engine.coordinator.PolicyDefinition;
import org.apache.eagle.alert.engine.coordinator.StreamDefinition;
import org.apache.eagle.alert.engine.evaluator.PolicyGroupEvaluator;
import org.apache.eagle.alert.engine.evaluator.impl.AlertBoltOutputCollectorSparkWrapper;
import org.apache.eagle.alert.engine.evaluator.impl.PolicyGroupEvaluatorImpl;
import org.apache.eagle.alert.engine.model.AlertStreamEvent;
import org.apache.eagle.alert.engine.model.PartitionedEvent;
import org.apache.eagle.alert.engine.runner.MapComparator;

import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;
import backtype.storm.metric.api.MultiCountMetric;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

public class AlertBoltFunction implements PairFlatMapFunction<Iterator<Tuple2<Integer, PartitionedEvent>>, String, AlertStreamEvent> {
    private static final Logger LOG = LoggerFactory.getLogger(AlertBoltFunction.class);

    private String alertBoltNamePrefix;
    private AtomicReference<Map<String, StreamDefinition>> sdsRef;
    private AtomicReference<AlertBoltSpec> alertBoltSpecRef;
    private int numOfAlertBolts;

    public AlertBoltFunction(String alertBoltNamePrefix, AtomicReference<Map<String, StreamDefinition>> sdsRef, AtomicReference<AlertBoltSpec> alertBoltSpecRef, int numOfAlertBolts) {
        this.alertBoltNamePrefix = alertBoltNamePrefix;
        this.sdsRef = sdsRef;
        this.alertBoltSpecRef = alertBoltSpecRef;
        this.numOfAlertBolts = numOfAlertBolts;
    }

    @Override
    public Iterator<Tuple2<String, AlertStreamEvent>> call(Iterator<Tuple2<Integer, PartitionedEvent>> tuple2Iterator) throws Exception {
        Map<String, StreamDefinition> sdf = sdsRef.get();
        AlertBoltSpec alertBoltSpec = alertBoltSpecRef.get();
        AlertBoltOutputCollectorSparkWrapper alertOutputCollector = new AlertBoltOutputCollectorSparkWrapper();
        PolicyGroupEvaluatorImpl[] evaluators = new PolicyGroupEvaluatorImpl[numOfAlertBolts];
        for (int i = 0; i < numOfAlertBolts; i++) {
            evaluators[i] = new PolicyGroupEvaluatorImpl(alertBoltNamePrefix + i);
            //TODO StreamContext need to be more abstract
            evaluators[i].init(new StreamContextImpl(null, new MultiCountMetric(), null), alertOutputCollector);
            onAlertBoltSpecChange(evaluators[i], alertBoltSpec, sdf);
        }

        while (tuple2Iterator.hasNext()) {
            Tuple2<Integer, PartitionedEvent> tuple2 = tuple2Iterator.next();
            PartitionedEvent event = tuple2._2;
            evaluators[tuple2._1].nextEvent(event);
        }

        cleanup(evaluators, alertOutputCollector);
        return alertOutputCollector.emitResult().iterator();
    }

    public void onAlertBoltSpecChange(PolicyGroupEvaluator policyGroupEvaluator, AlertBoltSpec spec, Map<String, StreamDefinition> sds) {

        Map<String, PolicyDefinition> cachedPolicies = new HashMap<>(); // for one streamGroup, there are multiple policies
        List<PolicyDefinition> newPolicies = spec.getBoltPoliciesMap().get(policyGroupEvaluator.getName());
        if (newPolicies == null) {
            LOG.info("no policy with AlertBoltSpec {} for this bolt {}", spec, policyGroupEvaluator.getName());
            return;
        }

        Map<String, PolicyDefinition> newPoliciesMap = new HashMap<>();
        newPolicies.forEach(p -> newPoliciesMap.put(p.getName(), p));
        MapComparator<String, PolicyDefinition> comparator = new MapComparator<>(newPoliciesMap, cachedPolicies);
        comparator.compare();
        policyGroupEvaluator.onPolicyChange(comparator.getAdded(), comparator.getRemoved(), comparator.getModified(), sds);
    }

    public void cleanup(PolicyGroupEvaluatorImpl[] policyGroupEvaluators, AlertBoltOutputCollectorSparkWrapper alertOutputCollector) {
        for (int i = 0; i < numOfAlertBolts; i++) {
            policyGroupEvaluators[i].close();
        }

        alertOutputCollector.flush();
        alertOutputCollector.close();
    }
}
