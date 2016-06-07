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
package org.apache.eagle.alert.coordination.model.internal;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.eagle.alert.engine.coordinator.StreamPartition;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.base.Objects;

/**
 * @since May 6, 2016
 *
 */
public class StreamGroup {

    private List<StreamPartition> streamPartitions = new ArrayList<StreamPartition>();
    
    public StreamGroup() {
    }
    
    public List<StreamPartition> getStreamPartitions() {
        return streamPartitions;
    }

    public void addStreamPartition(StreamPartition sp) {
        this.streamPartitions.add(sp);
    }

    public void addStreamPartitions(List<StreamPartition> sps) {
        this.streamPartitions.addAll(sps);
    }

    @org.codehaus.jackson.annotate.JsonIgnore
    @JsonIgnore
    public String getStreamId() {
        StringBuilder sb = new StringBuilder("SG[");
        for (StreamPartition sp : streamPartitions) {
            sb.append(sp.getStreamId()).append("-");
        }
        sb.append("]");
        return sb.toString();
    }

    @Override
    public int hashCode() {
        // implicitly all groups in stream groups will be built for hash code
        return new HashCodeBuilder().append(streamPartitions).build();
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof StreamGroup)) {
            return false;
        }
        StreamGroup o = (StreamGroup) obj;
        return Objects.equal(this.streamPartitions, o.streamPartitions);
    }

    @Override
    public String toString() {
        return String.format("StreamGroup partitions=: %s ", streamPartitions);
    }

}
