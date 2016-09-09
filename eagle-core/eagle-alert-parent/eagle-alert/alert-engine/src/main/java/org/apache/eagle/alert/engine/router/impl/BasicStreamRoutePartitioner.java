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

import org.apache.eagle.alert.engine.coordinator.StreamDefinition;
import org.apache.eagle.alert.engine.coordinator.StreamPartition;
import org.apache.eagle.alert.engine.model.StreamEvent;
import org.apache.eagle.alert.engine.router.StreamRoute;
import org.apache.eagle.alert.engine.router.StreamRoutePartitioner;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class BasicStreamRoutePartitioner implements StreamRoutePartitioner {
    private final List<String> outputComponentIds;
    private final StreamDefinition streamDefinition;
    private final StreamPartition streamPartition;

    public BasicStreamRoutePartitioner(List<String> outputComponentIds, StreamDefinition streamDefinition, StreamPartition partition) {
        this.outputComponentIds = outputComponentIds;
        this.streamDefinition = streamDefinition;
        this.streamPartition = partition;
    }

    @Override
    public List<StreamRoute> partition(StreamEvent event) {
        switch (this.streamPartition.getType()) {
            case GLOBAL:
                return routeToAll(event);
            case GROUPBY:
                return routeByGroupByKey(event);
            default:
                return routeByShuffle(event);
        }
    }

    protected List<StreamRoute> routeByGroupByKey(StreamEvent event) {
        int partitionKey = new HashCodeBuilder().append(event.getData(streamDefinition, this.streamPartition.getColumns())).build();
        String selectedOutputStream = outputComponentIds.get(Math.abs(partitionKey) % this.outputComponentIds.size());
        return Collections.singletonList(new StreamRoute(selectedOutputStream, partitionKey, StreamPartition.Type.GROUPBY));
    }

    protected List<StreamRoute> routeByShuffle(StreamEvent event) {
        long random = System.currentTimeMillis();
        int hash = Math.abs((int) random);
        return Arrays.asList(new StreamRoute(outputComponentIds.get(hash % outputComponentIds.size()), -1, StreamPartition.Type.SHUFFLE));
    }

    protected List<StreamRoute> routeToAll(StreamEvent event) {
        if (globalRoutingKeys != null) {
            globalRoutingKeys = new ArrayList<>();
            for (String targetId : outputComponentIds) {
                globalRoutingKeys.add(new StreamRoute(targetId, -1, StreamPartition.Type.GLOBAL));
            }
        }
        return globalRoutingKeys;
    }

    private List<StreamRoute> globalRoutingKeys = null;
}