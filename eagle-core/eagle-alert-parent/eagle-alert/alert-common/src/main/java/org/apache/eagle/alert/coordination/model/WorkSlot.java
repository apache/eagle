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


import org.apache.commons.lang3.builder.HashCodeBuilder;

import java.io.Serializable;
import java.util.Objects;

/**
 * A slot is simply a bolt.
 */
public class WorkSlot implements Serializable {
    public String topologyName;
    public String boltId;

    public String getTopologyName() {
        return topologyName;
    }

    public void setTopologyName(String topologyName) {
        this.topologyName = topologyName;
    }

    public String getBoltId() {
        return boltId;
    }

    public void setBoltId(String boltId) {
        this.boltId = boltId;
    }

    public WorkSlot() {

    }


    public WorkSlot(String topo, String boltId) {
        this.topologyName = topo;
        this.boltId = boltId;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof WorkSlot)) {
            return false;
        }
        WorkSlot workSlot = (WorkSlot) other;
        return Objects.equals(topologyName, workSlot.topologyName) &&
                Objects.equals(boltId, workSlot.boltId);
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder().append(topologyName).append(boltId).build();
    }

    public String toString() {
        return "(" + topologyName + ":" + boltId + ")";
    }
}