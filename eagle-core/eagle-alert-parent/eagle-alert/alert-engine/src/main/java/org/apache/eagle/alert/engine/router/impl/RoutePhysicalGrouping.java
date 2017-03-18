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
package org.apache.eagle.alert.engine.router.impl;

import org.apache.eagle.alert.engine.coordinator.StreamPartition;
import org.apache.eagle.alert.engine.model.PartitionedEvent;
import org.apache.storm.generated.GlobalStreamId;
import org.apache.storm.grouping.CustomStreamGrouping;
import org.apache.storm.task.WorkerTopologyContext;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.storm.trident.partition.GlobalGrouping;

import java.util.*;

public class RoutePhysicalGrouping implements CustomStreamGrouping {
    private static final long serialVersionUID = -511915083994148362L;
    private static final Logger LOG = LoggerFactory.getLogger(RoutePhysicalGrouping.class);
    private List<Integer> outdegreeTasks;
    private ShuffleGrouping shuffleGroupingDelegate;
    private GlobalGrouping globalGroupingDelegate;
    private Map<String, Integer> connectedTargetIds;

    @Override
    public void prepare(WorkerTopologyContext context, GlobalStreamId stream, List<Integer> targetTasks) {
        this.outdegreeTasks = new ArrayList<>(targetTasks);
        shuffleGroupingDelegate = new ShuffleGrouping();
        shuffleGroupingDelegate.prepare(context, stream, targetTasks);
        globalGroupingDelegate = new GlobalGrouping();
        globalGroupingDelegate.prepare(context, stream, targetTasks);
        connectedTargetIds = new HashMap<>();
        for (Integer targetId : targetTasks) {
            String targetComponentId = context.getComponentId(targetId);
            connectedTargetIds.put(targetComponentId, targetId);
        }
        LOG.info("OutDegree components: [{}]", StringUtils.join(connectedTargetIds.values(), ","));
    }

    @Override
    public List<Integer> chooseTasks(int taskId, List<Object> values) {
        Object routingKeyObj = values.get(0);
        if (routingKeyObj != null) {
            PartitionedEvent partitionedEvent = (PartitionedEvent) routingKeyObj;
            if (partitionedEvent.getPartition().getType() == StreamPartition.Type.GLOBAL) {
                return globalGroupingDelegate.chooseTasks(taskId, values);
            } else if (partitionedEvent.getPartition().getType() == StreamPartition.Type.GROUPBY) {
                return Collections.singletonList(outdegreeTasks.get((int) (partitionedEvent.getPartitionKey() % this.outdegreeTasks.size())));
            }
            // Shuffle by defaults
            return shuffleGroupingDelegate.chooseTasks(taskId, values);
        }

        LOG.warn("Illegal null StreamRoute, throw event");
        return Collections.emptyList();
    }
}