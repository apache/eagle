/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.eagle.alert.coordination.model;

import org.apache.eagle.alert.engine.coordinator.StreamPartition;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * One RouteSpec means one rule mapping [streamId -> StreamPartition ->
 * PolicyExecutionQueue]
 *
 * <p>Key is StreamPartition.
 */
public class StreamRouterSpec implements Serializable {
    private static final long serialVersionUID = 2315916073754999809L;
    private String streamId;
    private StreamPartition partition; // The meta-data to build
    // StreamPartitioner
    private List<PolicyWorkerQueue> targetQueue = new ArrayList<PolicyWorkerQueue>();

    public StreamPartition getPartition() {
        return partition;
    }

    public void setPartition(StreamPartition partition) {
        this.partition = partition;
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder().append(this.streamId).append(this.partition).append(targetQueue).build();
    }

    public List<PolicyWorkerQueue> getTargetQueue() {
        return targetQueue;
    }

    public void addQueue(PolicyWorkerQueue queue) {
        this.targetQueue.add(queue);
    }

    public String getStreamId() {
        return streamId;
    }

    public void setStreamId(String streamId) {
        this.streamId = streamId;
    }

    public void setTargetQueue(List<PolicyWorkerQueue> targetQueue) {
        this.targetQueue = targetQueue;
    }

    @Override
    public String toString() {
        return String.format("StreamRouterSpec[streamId=%s,partition=%s, queue=[%s]]", this.getStreamId(),
            this.getPartition(), this.getTargetQueue());
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof StreamRouterSpec)) {
            return false;
        }
        StreamRouterSpec that = (StreamRouterSpec) other;
        return Objects.equals(streamId, that.streamId)
            && Objects.equals(partition, that.partition)
            && CollectionUtils.isEqualCollection(targetQueue, that.targetQueue);
    }
}