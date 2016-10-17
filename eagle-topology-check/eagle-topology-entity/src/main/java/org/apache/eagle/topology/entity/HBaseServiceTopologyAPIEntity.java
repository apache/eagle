/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

package org.apache.eagle.topology.entity;

import org.apache.eagle.log.entity.meta.*;
import org.apache.eagle.topology.TopologyConstants;
import org.codehaus.jackson.map.annotate.JsonSerialize;

@JsonSerialize(include= JsonSerialize.Inclusion.NON_NULL)
@Table("hadoop_topology")
@ColumnFamily("f")
@Prefix("hbaseservicestatus")
@Service(TopologyConstants.HBASE_INSTANCE_SERVICE_NAME)
@TimeSeries(false)
public class HBaseServiceTopologyAPIEntity  extends TopologyBaseAPIEntity {
    @Column("a")
    private String status;
    @Column("b")
    private long maxHeapMB;
    @Column("c")
    private long usedHeapMB;
    @Column("d")
    private long numRegions;
    @Column("e")
    private long numRequests;
    @Column("f")
    private long lastUpdateTime;

    public long getLastUpdateTime() {
        return lastUpdateTime;
    }

    public void setLastUpdateTime(long lastUpdateTime) {
        this.lastUpdateTime = lastUpdateTime;
        valueChanged("lastUpdateTime");
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
        valueChanged("status");
    }

    public long getMaxHeapMB() {
        return maxHeapMB;
    }

    public void setMaxHeapMB(long maxHeapMB) {
        this.maxHeapMB = maxHeapMB;
        valueChanged("maxHeapMB");
    }

    public long getUsedHeapMB() {
        return usedHeapMB;
    }

    public void setUsedHeapMB(long usedHeapMB) {
        this.usedHeapMB = usedHeapMB;
        valueChanged("usedHeapMB");
    }

    public long getNumRegions() {
        return numRegions;
    }

    public void setNumRegions(long numRegions) {
        this.numRegions = numRegions;
        valueChanged("numRegions");
    }

    public long getNumRequests() {
        return numRequests;
    }

    public void setNumRequests(long numRequests) {
        this.numRequests = numRequests;
        valueChanged("numRequests");
    }

}
