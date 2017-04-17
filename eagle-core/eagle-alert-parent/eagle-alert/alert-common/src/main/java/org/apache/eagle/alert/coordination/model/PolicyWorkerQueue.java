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
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class PolicyWorkerQueue implements Serializable {

    private static final long serialVersionUID = 7131985778900576114L;
    private StreamPartition partition;
    private List<WorkSlot> workers;

    public PolicyWorkerQueue() {
        workers = new ArrayList<>();
    }

    public PolicyWorkerQueue(List<WorkSlot> workers) {
        this.workers = workers;
    }

    public PolicyWorkerQueue(StreamPartition partition, List<WorkSlot> workers) {
        this.workers = workers;
        this.partition = partition;
    }


    public StreamPartition getPartition() {
        return partition;
    }

    public void setPartition(StreamPartition partition) {
        this.partition = partition;
    }

    public List<WorkSlot> getWorkers() {
        return workers;
    }

    public void setWorkers(List<WorkSlot> workers) {
        this.workers = workers;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof PolicyWorkerQueue)) {
            return false;
        }
        PolicyWorkerQueue that = (PolicyWorkerQueue) other;
        return Objects.equals(partition, that.partition)
            && CollectionUtils.isEqualCollection(workers, that.workers);
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder().append(partition).append(workers).build();
    }

    public String toString() {
        return "[" + StringUtils.join(workers, ",") + "]";
    }

}
