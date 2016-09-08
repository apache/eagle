package org.apache.eagle.alert.engine.router;

import java.io.Serializable;

import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.eagle.alert.engine.coordinator.StreamPartition;

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
 * <p>
 * <b></b>
 * 1. Group by SingleStream[stream_1.col1]
 * <p>
 * Shuffle(stream_1,[col1])
 * <p>
 * <b></b>
 * 2. Group by SingleStream[stream_1.col1,stream_1.col2]
 * <p>
 * Shuffle(stream_1,[col1,col2])
 * <p>
 * <b></b>
 * 3. Group by JoinedStream[stream_1.col1,stream_1.col2,stream_2.col3]
 * <p>
 * Shuffle(stream_1.col1,stream_1.col2) + Global(stream_2.col3)
 */
public class StreamRoute implements Serializable {
    private static final long serialVersionUID = 4649184902196034940L;

    private String targetComponentId;
    private int partitionKey;
    private String partitionType;

    public String getTargetComponentId() {
        return targetComponentId;
    }

    public void setTargetComponentId(String targetComponentId) {
        this.targetComponentId = targetComponentId;
    }

    public StreamRoute(String targetComponentId, int partitionKey, StreamPartition.Type type) {
        this.setTargetComponentId(targetComponentId);
        this.setPartitionKey(partitionKey);
        this.setPartitionType(type);
    }

    public int getPartitionKey() {
        return partitionKey;
    }

    public void setPartitionKey(int partitionKey) {
        this.partitionKey = partitionKey;
    }

    public StreamPartition.Type getPartitionType() {
        return StreamPartition.Type.valueOf(partitionType);
    }

    public void setPartitionType(StreamPartition.Type partitionType) {
        this.partitionType = partitionType.name();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder().append(partitionKey).append(partitionType).append(targetComponentId).build();
    }

    @Override
    public String toString() {
        return String.format("Route[target=%s, key=%s, type=%s]", this.targetComponentId, this.partitionKey, this.partitionType);
    }
}