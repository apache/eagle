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

import backtype.storm.metric.api.MultiCountMetric;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;
import com.typesafe.config.Config;
import org.apache.commons.collections.CollectionUtils;
import org.apache.eagle.alert.coordination.model.PolicyWorkerQueue;
import org.apache.eagle.alert.coordination.model.RouterSpec;
import org.apache.eagle.alert.coordination.model.StreamRouterSpec;
import org.apache.eagle.alert.engine.StreamContext;
import org.apache.eagle.alert.engine.StreamContextImpl;
import org.apache.eagle.alert.engine.coordinator.*;
import org.apache.eagle.alert.engine.model.PartitionedEvent;
import org.apache.eagle.alert.engine.router.StreamRouter;
import org.apache.eagle.alert.engine.router.StreamRouterBoltSpecListener;
import org.apache.eagle.alert.engine.router.impl.StreamRouterBoltOutputCollector;
import org.apache.eagle.alert.engine.serialization.PartitionedEventSerializer;
import org.apache.eagle.alert.engine.serialization.SerializationMetadataProvider;
import org.apache.eagle.alert.engine.serialization.Serializers;
import org.apache.eagle.alert.utils.AlertConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

public class StreamRouterBolt extends AbstractStreamBolt implements StreamRouterBoltSpecListener, SerializationMetadataProvider{
    private final static Logger LOG = LoggerFactory.getLogger(StreamRouterBolt.class);
    private static final long serialVersionUID = -7611470889316430372L;
    private StreamRouter router;
    private StreamRouterBoltOutputCollector routeCollector;
    // mapping from StreamPartition to StreamSortSpec
    private volatile Map<StreamPartition, StreamSortSpec> cachedSSS = new HashMap<>();
    // mapping from StreamPartition(streamId, groupbyspec) to StreamRouterSpec
    private volatile Map<StreamPartition, StreamRouterSpec> cachedSRS = new HashMap<>();
    private volatile Map<String,StreamDefinition> sdf = new HashMap<>();
    private PartitionedEventSerializer serializer;

    public StreamRouterBolt(StreamRouter router, Config config, IMetadataChangeNotifyService changeNotifyService) {
        super(changeNotifyService, config);
        this.router = router;
    }

    private StreamContext streamContext;

    @Override
    public void internalPrepare(OutputCollector collector, IMetadataChangeNotifyService changeNotifyService, Config config, TopologyContext context) {
        streamContext = new StreamContextImpl(config,context.registerMetric("eagle.router",new MultiCountMetric(),60),context);
        serializer= Serializers.newPartitionedEventSerializer(this);
        routeCollector = new StreamRouterBoltOutputCollector(this.router.getName(),collector,this.getOutputStreamIds(),streamContext,serializer);
        router.prepare(streamContext, routeCollector);
        changeNotifyService.registerListener(this);
        changeNotifyService.init(config, MetadataType.STREAM_ROUTER_BOLT);
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
        try {
            this.streamContext.counter().scope("execute_count").incr();
            this.router.nextEvent(deserialize(input.getValueByField(AlertConstants.FIELD_0)).withAnchor(input));
        } catch (Exception ex) {
            this.streamContext.counter().scope("fail_count").incr();
            LOG.error(ex.getMessage(),ex);
            this.collector.fail(input);
        }
    }

    @Override
    public void cleanup() {
        this.router.close();
        super.cleanup();
    }

    /**
     * Compare with metadata snapshot cache to generate diff like added, removed and modified between different versions.
     * @param spec
     */
    @SuppressWarnings("unchecked")
    @Override
    public void onStreamRouteBoltSpecChange(RouterSpec spec, Map<String, StreamDefinition> sds) {
        sanityCheck(spec);

        // figure out added, removed, modified StreamSortSpec
        Map<StreamPartition, StreamSortSpec> newSSS = new HashMap<>();
        spec.getRouterSpecs().forEach(t ->  {
            if (t.getPartition().getSortSpec() != null) {
                newSSS.put(t.getPartition(), t.getPartition().getSortSpec());
            }
        });

        Set<StreamPartition> newStreamIds = newSSS.keySet();
        Set<StreamPartition> cachedStreamIds = cachedSSS.keySet();
        Collection<StreamPartition> addedStreamIds = CollectionUtils.subtract(newStreamIds, cachedStreamIds);
        Collection<StreamPartition> removedStreamIds = CollectionUtils.subtract(cachedStreamIds, newStreamIds);
        Collection<StreamPartition> modifiedStreamIds = CollectionUtils.intersection(newStreamIds, cachedStreamIds);

        Map<StreamPartition, StreamSortSpec> added = new HashMap<>();
        Map<StreamPartition, StreamSortSpec> removed = new HashMap<>();
        Map<StreamPartition, StreamSortSpec> modified = new HashMap<>();
        addedStreamIds.forEach(s -> added.put(s, newSSS.get(s)));
        removedStreamIds.forEach(s -> removed.put(s, cachedSSS.get(s)));
        modifiedStreamIds.forEach(s -> {
            if(!newSSS.get(s).equals(cachedSSS.get(s))){ // this means StreamSortSpec is changed for one specific streamId
                modified.put(s, newSSS.get(s));
            }
        });
        if(LOG.isDebugEnabled()) {
            LOG.debug("added StreamSortSpec " + added);
            LOG.debug("removed StreamSortSpec " + removed);
            LOG.debug("modified StreamSortSpec " + modified);
        }
        router.onStreamSortSpecChange(added, removed, modified);
        // switch cache
        cachedSSS = newSSS;

        // figure out added, removed, modified StreamRouterSpec
        Map<StreamPartition, StreamRouterSpec> newSRS = new HashMap<>();
        spec.getRouterSpecs().forEach(t -> newSRS.put(t.getPartition(), t));

        Set<StreamPartition> newStreamPartitions = newSRS.keySet();
        Set<StreamPartition> cachedStreamPartitions = cachedSRS.keySet();

        Collection<StreamPartition> addedStreamPartitions = CollectionUtils.subtract(newStreamPartitions, cachedStreamPartitions);
        Collection<StreamPartition> removedStreamPartitions = CollectionUtils.subtract(cachedStreamPartitions, newStreamPartitions);
        Collection<StreamPartition> modifiedStreamPartitions = CollectionUtils.intersection(newStreamPartitions, cachedStreamPartitions);

        Collection<StreamRouterSpec> addedRouterSpecs = new ArrayList<>();
        Collection<StreamRouterSpec> removedRouterSpecs = new ArrayList<>();
        Collection<StreamRouterSpec> modifiedRouterSpecs = new ArrayList<>();
        addedStreamPartitions.forEach(s -> addedRouterSpecs.add(newSRS.get(s)));
        removedStreamPartitions.forEach(s -> removedRouterSpecs.add(cachedSRS.get(s)));
        modifiedStreamPartitions.forEach(s -> {
            if(!newSRS.get(s).equals(cachedSRS.get(s))){ // this means StreamRouterSpec is changed for one specific StreamPartition
                modifiedRouterSpecs.add(newSRS.get(s));
            }
        });

        if(LOG.isDebugEnabled()) {
            LOG.debug("added StreamRouterSpec " + addedRouterSpecs);
            LOG.debug("removed StreamRouterSpec " + removedRouterSpecs);
            LOG.debug("modified StreamRouterSpec " + modifiedRouterSpecs);
        }

        routeCollector.onStreamRouterSpecChange(addedRouterSpecs, removedRouterSpecs, modifiedRouterSpecs, sds);
        // switch cache
        cachedSRS = newSRS;
        sdf = sds;
    }

    /**
     * in correlation cases, multiple streams will go to the same queue for correlation policy
     * @param spec
     */
    private void sanityCheck(RouterSpec spec){
        Set<String> totalRequestedSlots = new HashSet<>();
        for(StreamRouterSpec s : spec.getRouterSpecs()){
            for(PolicyWorkerQueue q : s.getTargetQueue()){
                List<String> workers = new ArrayList<>();
                q.getWorkers().forEach(w -> workers.add(w.getBoltId()));
                totalRequestedSlots.addAll(workers);
            }
        }
        if(totalRequestedSlots.size() > getOutputStreamIds().size()){
            String error = String.format("Requested slots are not consistent with provided slots, %s, %s", totalRequestedSlots, getOutputStreamIds());
            LOG.error(error);
            throw new IllegalStateException(error);
        }
    }

    @Override
    public StreamDefinition getStreamDefinition(String streamId) {
        return this.sdf.get(streamId);
    }
}