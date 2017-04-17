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

import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import org.apache.eagle.log.entity.meta.*;
import org.apache.eagle.topology.TopologyConstants;


@JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)
@Table("hadoop_topology")
@ColumnFamily("f")
@Prefix("systemservicestatus")
@Service(TopologyConstants.SYSTEM_INSTANCE_SERVICE_NAME)
@TimeSeries(false)
public class SystemServiceTopologyAPIEntity extends TopologyBaseAPIEntity {
    @Column("a")
    private String status;
    @Column("b")
    private String totalMemoryMB;
    @Column("c")
    private String usedMemoryMB;
    @Column("d")
    private String totalDiskGB;
    @Column("e")
    private String usedDiskGB;
    @Column("f")
    private long lastUpdateTime;
    @Column("g")
    private String cores;

    private String host;

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
        valueChanged("status");
    }

    public String getTotalMemoryMB() {
        return totalMemoryMB;
    }

    public void setTotalMemoryMB(String totalMemoryMB) {
        this.totalMemoryMB = totalMemoryMB;
        valueChanged("totalMemoryMB");
    }

    public String getUsedMemoryMB() {
        return usedMemoryMB;
    }

    public void setUsedMemoryMB(String usedMemoryMB) {
        this.usedMemoryMB = usedMemoryMB;
        valueChanged("usedMemoryMB");
    }

    public String getTotalDiskGB() {
        return totalDiskGB;
    }

    public void setTotalDiskGB(String totalDiskGB) {
        this.totalDiskGB = totalDiskGB;
        valueChanged("totalDiskGB");
    }

    public String getUsedDiskGB() {
        return usedDiskGB;
    }

    public void setUsedDiskGB(String usedDiskGB) {
        this.usedDiskGB = usedDiskGB;
        valueChanged("usedDiskGB");
    }

    public long getLastUpdateTime() {
        return lastUpdateTime;
    }

    public void setLastUpdateTime(long lastUpdateTime) {
        this.lastUpdateTime = lastUpdateTime;
        valueChanged("lastUpdateTime");
    }

    public String getCores() {
        return cores;
    }

    public void setCores(String cores) {
        this.cores = cores;
        valueChanged("cores");
    }

}

