package org.apache.eagle.alert.engine.router.impl;

import com.google.common.collect.Lists;
import org.apache.eagle.alert.coordination.model.PolicyWorkerQueue;
import org.apache.eagle.alert.coordination.model.StreamRouterSpec;
import org.apache.eagle.alert.coordination.model.WorkSlot;
import org.apache.eagle.alert.engine.PartitionedEventCollector;
import org.apache.eagle.alert.engine.coordinator.StreamDefinition;
import org.apache.eagle.alert.engine.coordinator.StreamPartition;
import org.apache.eagle.alert.engine.model.PartitionedEvent;
import org.apache.eagle.alert.engine.model.StreamEvent;
import org.apache.eagle.alert.engine.router.StreamRoute;
import org.apache.eagle.alert.engine.router.StreamRoutePartitionFactory;
import org.apache.eagle.alert.engine.router.StreamRoutePartitioner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.Serializable;
import java.util.*;


public class SparkStreamRouterBoltOutputCollector implements PartitionedEventCollector, Serializable {
    private final static Logger LOG = LoggerFactory.getLogger(SparkStreamRouterBoltOutputCollector.class);
    private final List<Tuple2<Integer, Object>> outputCollector;
    private volatile Map<StreamPartition, StreamRouterSpec> routeSpecMap;
    private volatile Map<StreamPartition, List<StreamRoutePartitioner>> routePartitionerMap;
    // private final String outputLock = new String("LOCK");
    private final String sourceId;

    public SparkStreamRouterBoltOutputCollector(String sourceId) {
        this.sourceId = sourceId;
        this.outputCollector = new LinkedList<Tuple2<Integer, Object>>();
        this.routeSpecMap = new HashMap<>();
        this.routePartitionerMap = new HashMap<>();
    }

    public List<Tuple2<Integer, Object>> emitResult() {
        if (outputCollector.isEmpty()) {
            return Collections.emptyList();
        }
        List<Tuple2<Integer, Object>> result = new LinkedList<>();
        result.addAll(outputCollector);
        outputCollector.clear();
        return result;
    }

    public void emit(PartitionedEvent event) {
        try {
            StreamPartition partition = event.getPartition();
            StreamRouterSpec routerSpec = routeSpecMap.get(partition);
            if (routerSpec == null) {
                if (LOG.isDebugEnabled()) {
                    // Don't know how to route stream, if it's correct, it's better to filter useless stream in spout side
                    LOG.debug("Drop event {} as StreamPartition {} is not pointed to any router metadata {}", event, event.getPartition(), routeSpecMap);
                }
                this.drop(event);
                return;
            }

            if (routePartitionerMap.get(routerSpec.getPartition()) == null) {
                LOG.error("Partitioner for " + routerSpec + " is null");
                return;
            }

            StreamEvent newEvent = event.getEvent().copy();

            // Get handler for the partition
            List<StreamRoutePartitioner> queuePartitioners = routePartitionerMap.get(partition);

            //  synchronized (outputLock) {
            for (StreamRoutePartitioner queuePartitioner : queuePartitioners) {
                List<StreamRoute> streamRoutes = queuePartitioner.partition(newEvent);
                // it is possible that one event can be sent to multiple slots in one slotqueue if that is All grouping
                for (StreamRoute streamRoute : streamRoutes) {
                    int partitionIndex = getPartitionIndex(streamRoute);
                    try {
                        PartitionedEvent emittedEvent = new PartitionedEvent(newEvent, routerSpec.getPartition(), streamRoute.getPartitionKey());
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("Emitted to partition {} with message {}", partitionIndex, emittedEvent);
                        }
                        outputCollector.add(new Tuple2<Integer, Object>(partitionIndex, event));
                    } catch (RuntimeException ex) {
                        LOG.error("Failed to emit to partition {} with {}", partitionIndex, newEvent, ex);
                        throw ex;
                    }
                }
            }
        } catch (Exception ex) {
            LOG.error(ex.getMessage(), ex);
        }
    }

    private Integer getPartitionIndex(StreamRoute streamRoute) {
        return Integer.valueOf(streamRoute.getTargetComponentId().replaceAll("[^\\d.]", ""));
    }

    public void onStreamRouterSpecChange(Collection<StreamRouterSpec> added,
                                         Collection<StreamRouterSpec> removed,
                                         Collection<StreamRouterSpec> modified,
                                         Map<String, StreamDefinition> sds) {
        Map<StreamPartition, StreamRouterSpec> copyRouteSpecMap = new HashMap<>(routeSpecMap);
        Map<StreamPartition, List<StreamRoutePartitioner>> copyRoutePartitionerMap = new HashMap<>(routePartitionerMap);

        // added StreamRouterSpec i.e. there is a new StreamPartition
        for (StreamRouterSpec spec : added) {
            if (copyRouteSpecMap.containsKey(spec.getPartition())) {
                LOG.error("Metadata calculation error: add existing StreamRouterSpec " + spec);
            } else {
                inplaceAdd(copyRouteSpecMap, copyRoutePartitionerMap, spec, sds);
            }
        }

        // removed StreamRouterSpec i.e. there is a deleted StreamPartition
        for (StreamRouterSpec spec : removed) {
            if (!copyRouteSpecMap.containsKey(spec.getPartition())) {
                LOG.error("Metadata calculation error: remove non-existing StreamRouterSpec " + spec);
            } else {
                inplaceRemove(copyRouteSpecMap, copyRoutePartitionerMap, spec);
            }
        }

        // modified StreamRouterSpec, i.e. there is modified StreamPartition, for example WorkSlotQueue assignment is changed
        for (StreamRouterSpec spec : modified) {
            if (!copyRouteSpecMap.containsKey(spec.getPartition())) {
                LOG.error("Metadata calculation error: modify nonexisting StreamRouterSpec " + spec);
            } else {
                inplaceRemove(copyRouteSpecMap, copyRoutePartitionerMap, spec);
                inplaceAdd(copyRouteSpecMap, copyRoutePartitionerMap, spec, sds);
            }
        }

        // switch
        routeSpecMap = copyRouteSpecMap;
        routePartitionerMap = copyRoutePartitionerMap;
    }

    private void inplaceRemove(Map<StreamPartition, StreamRouterSpec> routeSpecMap,
                               Map<StreamPartition, List<StreamRoutePartitioner>> routePartitionerMap,
                               StreamRouterSpec toBeRemoved) {
        routeSpecMap.remove(toBeRemoved.getPartition());
        routePartitionerMap.remove(toBeRemoved.getPartition());
    }

    private void inplaceAdd(Map<StreamPartition, StreamRouterSpec> routeSpecMap,
                            Map<StreamPartition, List<StreamRoutePartitioner>> routePartitionerMap,
                            StreamRouterSpec toBeAdded, Map<String, StreamDefinition> sds) {
        routeSpecMap.put(toBeAdded.getPartition(), toBeAdded);
        try {
            List<StreamRoutePartitioner> routePartitioners = calculatePartitioner(toBeAdded, sds);
            routePartitionerMap.put(toBeAdded.getPartition(), routePartitioners);
        } catch (Exception e) {
            LOG.error("ignore this failure StreamRouterSpec " + toBeAdded + ", with error" + e.getMessage(), e);
            routeSpecMap.remove(toBeAdded.getPartition());
            routePartitionerMap.remove(toBeAdded.getPartition());
        }
    }

    private List<StreamRoutePartitioner> calculatePartitioner(StreamRouterSpec streamRouterSpec, Map<String, StreamDefinition> sds) throws Exception {
        List<StreamRoutePartitioner> routePartitioners = new ArrayList<>();
        for (PolicyWorkerQueue pwq : streamRouterSpec.getTargetQueue()) {
            routePartitioners.add(StreamRoutePartitionFactory.createRoutePartitioner(
                    Lists.transform(pwq.getWorkers(), WorkSlot::getBoltId),
                    sds.get(streamRouterSpec.getPartition().getStreamId()),
                    streamRouterSpec.getPartition()));
        }
        return routePartitioners;
    }

    @Override
    public void drop(PartitionedEvent event) {
    }

    public void flush() {

    }
}
