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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.collections.CollectionUtils;
import org.apache.eagle.alert.engine.coordinator.StreamPartition;

public class StreamRepartitionStrategy  implements Serializable {
    public StreamPartition partition ;

    public int numTotalParticipatingRouterBolts = 0;      // how many group-by bolts participate policy evaluation
    public int startSequence = 0;            // what is the sequence for the first bolt in this topology among all bolts
    public List<String> totalTargetBoltIds = new ArrayList<String>();
    
    public int hashCode() {
        int hashcode = 1 * 31;
        hashcode += partition.hashCode();
        for (String str : totalTargetBoltIds) {
            hashcode += str.hashCode();
        }
        return hashcode;
    }
    
    public boolean equals(Object obj) {
        if (!(obj instanceof StreamRepartitionStrategy)) {
            return false;
        }
        StreamRepartitionStrategy o = (StreamRepartitionStrategy) obj;
        return partition.equals(o.partition)
                && CollectionUtils.isEqualCollection(totalTargetBoltIds, o.totalTargetBoltIds);
    }

    public StreamPartition getPartition() {
        return partition;
    }

    public void setPartition(StreamPartition partition) {
        this.partition = partition;
    }

    public int getNumTotalParticipatingRouterBolts() {
        return numTotalParticipatingRouterBolts;
    }

    public void setNumTotalParticipatingRouterBolts(int numTotalParticipatingRouterBolts) {
        this.numTotalParticipatingRouterBolts = numTotalParticipatingRouterBolts;
    }

    public int getStartSequence() {
        return startSequence;
    }

    public void setStartSequence(int startSequence) {
        this.startSequence = startSequence;
    }

    public List<String> getTotalTargetBoltIds() {
        return totalTargetBoltIds;
    }

    public void setTotalTargetBoltIds(List<String> totalTargetBoltIds) {
        this.totalTargetBoltIds = totalTargetBoltIds;
    }

}