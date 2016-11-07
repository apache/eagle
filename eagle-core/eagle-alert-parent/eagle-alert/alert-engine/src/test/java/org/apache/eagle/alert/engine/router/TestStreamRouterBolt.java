package org.apache.eagle.alert.engine.router;

import org.apache.eagle.alert.coordination.model.PolicyWorkerQueue;
import org.apache.eagle.alert.coordination.model.StreamRouterSpec;
import org.apache.eagle.alert.coordination.model.WorkSlot;
import org.apache.eagle.alert.engine.coordinator.StreamPartition;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class TestStreamRouterBolt {
    private static final Logger LOG = LoggerFactory.getLogger(TestStreamRouterBolt.class);
    Map<StreamPartition, List<StreamRouterSpec>>  routeSpecMap = new HashMap<>();

    @Test
    public void testStreamRouterSpecChange() {
        StreamPartition partition = new StreamPartition();
        partition.setStreamId("HDFS_AUDIT_LOG_ENRICHED_STREAM_SANDBOX");
        partition.setType(StreamPartition.Type.SHUFFLE);

        WorkSlot worker1 = new WorkSlot("ALERT_UNIT_TOPOLOGY_APP_SANDBOX", "alertBolt1");
        WorkSlot worker2 = new WorkSlot("ALERT_UNIT_TOPOLOGY_APP_SANDBOX", "alertBolt2");

        PolicyWorkerQueue queue1 = new PolicyWorkerQueue();
        queue1.setPartition(partition);
        queue1.setWorkers(new ArrayList<WorkSlot>(){ {
            add(worker1);
        }} );

        PolicyWorkerQueue queue2 = new PolicyWorkerQueue();
        queue2.setPartition(partition);
        queue2.setWorkers(new ArrayList<WorkSlot>(){ {
            add(worker1);
        }} );

        PolicyWorkerQueue queue3 = new PolicyWorkerQueue();
        queue3.setPartition(partition);
        queue3.setWorkers(new ArrayList<WorkSlot>(){ {
            add(worker1);
            add(worker2);
        }} );

        StreamRouterSpec spec1 = new StreamRouterSpec();
        spec1.setStreamId("HDFS_AUDIT_LOG_ENRICHED_STREAM_SANDBOX");
        spec1.setPartition(partition);

        spec1.setTargetQueue(new ArrayList<PolicyWorkerQueue>(){{
            add(queue1);
        }});

        StreamRouterSpec spec2 = new StreamRouterSpec();
        spec2.setStreamId("HDFS_AUDIT_LOG_ENRICHED_STREAM_SANDBOX");
        spec2.setPartition(partition);

        spec2.setTargetQueue(new ArrayList<PolicyWorkerQueue>(){{
            add(queue2);
        }});

        StreamRouterSpec spec3 = new StreamRouterSpec();
        spec3.setStreamId("HDFS_AUDIT_LOG_ENRICHED_STREAM_SANDBOX");
        spec3.setPartition(partition);

        spec3.setTargetQueue(new ArrayList<PolicyWorkerQueue>(){{
            add(queue3);
        }});


        List<StreamRouterSpec> list1 = new ArrayList<>();
        list1.add(spec1);

        List<StreamRouterSpec> list2 = new ArrayList<>();
        list2.add(spec2);

        List<StreamRouterSpec> list3 = new ArrayList<>();
        list3.add(spec3);

        // test add
        onStreamRouterSpecChange(list1, new ArrayList<>(), new ArrayList<>());
        Assert.assertTrue(routeSpecMap.size() == 1);

        // test modified
        onStreamRouterSpecChange(new ArrayList<>(), new ArrayList<>(), list3);
        Assert.assertTrue(routeSpecMap.get(partition).get(0).getTargetQueue().get(0).getWorkers().size() == 2);

        // test remove
        onStreamRouterSpecChange(new ArrayList<>(), list1, new ArrayList<>());
        Assert.assertTrue(routeSpecMap.size() == 1);

        onStreamRouterSpecChange(new ArrayList<>(), list3, new ArrayList<>());
        Assert.assertTrue(routeSpecMap.size() == 0);
    }


    private void onStreamRouterSpecChange(Collection<StreamRouterSpec> added,
                                         Collection<StreamRouterSpec> removed,
                                         Collection<StreamRouterSpec> modified) {
        Map<StreamPartition, List<StreamRouterSpec>> copyRouteSpecMap = new HashMap<>(routeSpecMap);

        // added StreamRouterSpec i.e. there is a new StreamPartition
        for (StreamRouterSpec spec : added) {
            if (copyRouteSpecMap.containsKey(spec.getPartition())
                    && copyRouteSpecMap.get(spec.getPartition()).contains(spec)) {
                LOG.error("Metadata calculation error: add existing StreamRouterSpec " + spec);
            } else {
                inplaceAdd(copyRouteSpecMap, spec);
            }
        }

        // removed StreamRouterSpec i.e. there is a deleted StreamPartition
        for (StreamRouterSpec spec : removed) {
            if (!copyRouteSpecMap.containsKey(spec.getPartition())
                    || !copyRouteSpecMap.get(spec.getPartition()).contains(spec)) {
                LOG.error("Metadata calculation error: remove non-existing StreamRouterSpec " + spec);
            } else {
                inplaceRemove(copyRouteSpecMap, spec);
            }
        }

        // modified StreamRouterSpec, i.e. there is modified StreamPartition, for example WorkSlotQueue assignment is changed
        for (StreamRouterSpec spec : modified) {
            if (!copyRouteSpecMap.containsKey(spec.getPartition())
                    || copyRouteSpecMap.get(spec.getPartition()).contains(spec)) {
                LOG.error("Metadata calculation error: modify nonexisting StreamRouterSpec " + spec);
            } else {
                inplaceRemove(copyRouteSpecMap, spec);
                inplaceAdd(copyRouteSpecMap, spec);
            }
        }

        // switch
        routeSpecMap = copyRouteSpecMap;
    }

    private void inplaceRemove(Map<StreamPartition, List<StreamRouterSpec>> routeSpecMap,
                               StreamRouterSpec toBeRemoved) {
        routeSpecMap.remove(toBeRemoved.getPartition());
    }

    private void inplaceAdd(Map<StreamPartition, List<StreamRouterSpec>> routeSpecMap,
                            StreamRouterSpec toBeAdded) {
        if (!routeSpecMap.containsKey(toBeAdded.getPartition())) {
            routeSpecMap.put(toBeAdded.getPartition(), new ArrayList<StreamRouterSpec>());
        }
        routeSpecMap.get(toBeAdded.getPartition()).add(toBeAdded);
    }
}
