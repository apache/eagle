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
package org.apache.eagle.alert.engine.model;

import org.apache.eagle.alert.engine.coordinator.StreamPartition;
import backtype.storm.tuple.Tuple;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import java.io.Serializable;
import java.util.Objects;

/**
 * This is a critical data structure across spout, router bolt and alert bolt
 * partition[StreamPartition] defines how one incoming data stream is partitioned, sorted
 * partitionKey[long] is java hash value of groupby fields. The groupby fields are defined in StreamPartition
 * event[StreamEvent] is actual data.
 */
public class PartitionedEvent implements Serializable {
    private static final long serialVersionUID = -3840016190614238593L;
    private StreamPartition partition;
    private long partitionKey;
    private StreamEvent event;

    /**
     * Used for bolt-internal but not inter-bolts,
     * will not pass across bolts.
     */
    private transient Tuple anchor;

    public PartitionedEvent() {
        this.event = null;
        this.partition = null;
        this.partitionKey = 0L;
    }

    public PartitionedEvent(StreamEvent event, StreamPartition partition, int partitionKey) {
        this.event = event;
        this.partition = partition;
        this.partitionKey = partitionKey;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (obj instanceof PartitionedEvent) {
            PartitionedEvent another = (PartitionedEvent) obj;
            return !(this.partitionKey != another.getPartitionKey()
                || !Objects.equals(this.event, another.getEvent())
                || !Objects.equals(this.partition, another.getPartition())
                || !Objects.equals(this.anchor, another.anchor));
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder()
            .append(partitionKey)
            .append(event)
            .append(partition)
            .build();
    }

    public StreamEvent getEvent() {
        return event;
    }

    public void setEvent(StreamEvent event) {
        this.event = event;
    }

    public StreamPartition getPartition() {
        return partition;
    }

    public void setPartition(StreamPartition partition) {
        this.partition = partition;
    }

    public void setPartitionKey(long partitionKey) {
        this.partitionKey = partitionKey;
    }

    public long getPartitionKey() {
        return this.partitionKey;
    }

    public String toString() {
        return String.format("PartitionedEvent[partition=%s,event=%s,key=%s", partition, event, partitionKey);
    }

    public long getTimestamp() {
        return (event != null) ? event.getTimestamp() : 0L;
    }

    public String getStreamId() {
        return (event != null) ? event.getStreamId() : null;
    }

    public Object[] getData() {
        return event != null ? event.getData() : null;
    }

    public boolean isSortRequired() {
        return isPartitionRequired() && this.getPartition().getSortSpec() != null;
    }

    public boolean isPartitionRequired() {
        return this.getPartition() != null;
    }

    public PartitionedEvent copy() {
        PartitionedEvent copied = new PartitionedEvent();
        copied.setEvent(this.getEvent());
        copied.setPartition(this.partition);
        copied.setPartitionKey(this.partitionKey);
        return copied;
    }

    public Tuple getAnchor() {
        return anchor;
    }

    public void setAnchor(Tuple anchor) {
        this.anchor = anchor;
    }

    public PartitionedEvent withAnchor(Tuple tuple) {
        this.setAnchor(tuple);
        return this;
    }
}