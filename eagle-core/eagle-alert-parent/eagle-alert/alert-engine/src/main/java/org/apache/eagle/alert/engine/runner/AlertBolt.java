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
package org.apache.eagle.alert.engine.runner;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.eagle.alert.coordination.model.AlertBoltSpec;
import org.apache.eagle.alert.coordination.model.WorkSlot;
import org.apache.eagle.alert.engine.AlertStreamCollector;
import org.apache.eagle.alert.engine.StreamContext;
import org.apache.eagle.alert.engine.StreamContextImpl;
import org.apache.eagle.alert.engine.coordinator.IMetadataChangeNotifyService;
import org.apache.eagle.alert.engine.coordinator.MetadataType;
import org.apache.eagle.alert.engine.coordinator.PolicyDefinition;
import org.apache.eagle.alert.engine.coordinator.StreamDefinition;
import org.apache.eagle.alert.engine.evaluator.PolicyGroupEvaluator;
import org.apache.eagle.alert.engine.evaluator.impl.AlertBoltOutputCollectorWrapper;
import org.apache.eagle.alert.engine.model.PartitionedEvent;
import org.apache.eagle.alert.engine.router.AlertBoltSpecListener;
import org.apache.eagle.alert.engine.serialization.PartitionedEventSerializer;
import org.apache.eagle.alert.engine.serialization.SerializationMetadataProvider;
import org.apache.eagle.alert.engine.serialization.Serializers;
import org.apache.eagle.alert.utils.AlertConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.metric.api.MultiCountMetric;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

import com.typesafe.config.Config;

/**
 * Since 5/1/16.
 * This is container for hosting all policies belonging to the same monitoredStream
 * MonitoredStream refers to tuple of {dataSource, streamId, grouopby}
 * The container is also called {@link WorkSlot}
 */
public class AlertBolt extends AbstractStreamBolt implements AlertBoltSpecListener,SerializationMetadataProvider {
    private final static Logger LOG = LoggerFactory.getLogger(AlertBolt.class);
    private static final long serialVersionUID = -4132297691448945672L;
    private PolicyGroupEvaluator policyGroupEvaluator;
    private AlertStreamCollector alertOutputCollector;
    private String boltId;
    private volatile Object outputLock;
    // mapping from policy name to PolicyDefinition
    private volatile Map<String, PolicyDefinition> cachedPolicies = new HashMap<>(); // for one streamGroup, there are multiple policies

    private StreamContext streamContext;
    private volatile Map<String, StreamDefinition> sdf  = new HashMap<String, StreamDefinition>();
    private PartitionedEventSerializer serializer;

    public AlertBolt(String boltId, PolicyGroupEvaluator policyGroupEvaluator, Config config, IMetadataChangeNotifyService changeNotifyService){
        super(changeNotifyService, config);
        this.boltId = boltId;
        this.policyGroupEvaluator = policyGroupEvaluator;
    }

    PartitionedEvent deserialize(Object object) throws IOException {
        // byte[] in higher priority
        if(object instanceof byte[]) {
            return serializer.deserialize((byte[]) object);
        } else {
            return (PartitionedEvent) object;
        }
    }

    @Override
    public void execute(Tuple input) {
        this.streamContext.counter().scope("execute_count").incr();
        try {
            policyGroupEvaluator.nextEvent(deserialize(input.getValueByField(AlertConstants.FIELD_0)).withAnchor(input));
            synchronized (outputLock) {
                this.collector.ack(input);
            }
            this.streamContext.counter().scope("ack_count").incr();
        }catch (Exception ex) {
            LOG.error(ex.getMessage(),ex);
            synchronized (outputLock) {
                this.streamContext.counter().scope("fail_count").incr();
                this.collector.fail(input);
            }
        } finally {
            alertOutputCollector.flush();
        }
    }

    @Override
    public void internalPrepare(OutputCollector collector, IMetadataChangeNotifyService metadataChangeNotifyService, Config config, TopologyContext context) {
        // instantiate output lock object
        outputLock = new Object();
        streamContext = new StreamContextImpl(config,context.registerMetric("eagle.evaluator",new MultiCountMetric(),60),context);
        serializer = Serializers.newPartitionedEventSerializer(this);
        alertOutputCollector = new AlertBoltOutputCollectorWrapper(collector, outputLock,streamContext);
        policyGroupEvaluator.init(streamContext, alertOutputCollector);
        metadataChangeNotifyService.registerListener(this);
        metadataChangeNotifyService.init(config, MetadataType.ALERT_BOLT);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(AlertConstants.FIELD_0, AlertConstants.FIELD_1));
    }

    @Override
    public void cleanup() {
        policyGroupEvaluator.close();
        alertOutputCollector.flush();
        alertOutputCollector.close();
        super.cleanup();
    }

    @Override
    public void onAlertBoltSpecChange(AlertBoltSpec spec, Map<String, StreamDefinition> sds) {
        List<PolicyDefinition> newPolicies = spec.getBoltPoliciesMap().get(boltId);
        if(newPolicies == null) {
            LOG.info("no new policy with AlertBoltSpec {} for this bolt {}", spec, boltId);
            return;
        }

        Map<String, PolicyDefinition> newPoliciesMap = new HashMap<>();
        newPolicies.forEach(p -> newPoliciesMap.put(p.getName(), p));
        MapComparator<String, PolicyDefinition> comparator = new MapComparator<>(newPoliciesMap, cachedPolicies);
        comparator.compare();

        policyGroupEvaluator.onPolicyChange(comparator.getAdded(), comparator.getRemoved(), comparator.getModified(), sds);

        // switch
        cachedPolicies = newPoliciesMap;
        sdf = sds;
    }

    @Override
    public StreamDefinition getStreamDefinition(String streamId) {
        return sdf.get(streamId);
    }
}