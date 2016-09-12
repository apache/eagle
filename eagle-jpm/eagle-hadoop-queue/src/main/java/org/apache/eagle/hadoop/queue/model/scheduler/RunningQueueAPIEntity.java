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

package org.apache.eagle.hadoop.queue.model.scheduler;

import org.apache.eagle.hadoop.queue.common.HadoopClusterConstants;
import org.apache.eagle.log.base.taggedlog.TaggedLogAPIEntity;
import org.apache.eagle.log.entity.meta.*;
import org.codehaus.jackson.map.annotate.JsonSerialize;

import java.util.List;

@JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)
@Table("running_queue")
@ColumnFamily("f")
@Prefix("rqueue")
@Service(HadoopClusterConstants.RUNNING_QUEUE_SERVICE_NAME)
@TimeSeries(true)
@Partition( {"site"})
public class RunningQueueAPIEntity extends TaggedLogAPIEntity {
    @Column("a")
    private String state;
    @Column("b")
    private double absoluteCapacity;
    @Column("c")
    private double absoluteMaxCapacity;
    @Column("d")
    private double absoluteUsedCapacity;
    @Column("e")
    private long memory;
    @Column("f")
    private long vcores;
    @Column("g")
    private int numActiveApplications;
    @Column("h")
    private int numPendingApplications;
    @Column("i")
    private int maxActiveApplications;
    @Column("j")
    private String scheduler;
    @Column("k")
    private List<UserWrapper> users;

    public String getScheduler() {
        return scheduler;
    }

    public void setScheduler(String scheduler) {
        this.scheduler = scheduler;
        valueChanged("scheduler");
    }

    public int getMaxActiveApplications() {
        return maxActiveApplications;
    }

    public void setMaxActiveApplications(int maxActiveApplications) {
        this.maxActiveApplications = maxActiveApplications;
        valueChanged("maxActiveApplications");
    }

    public String getState() {
        return state;
    }

    public void setState(String state) {
        this.state = state;
        valueChanged("state");

    }

    public double getAbsoluteCapacity() {
        return absoluteCapacity;
    }

    public void setAbsoluteCapacity(double absoluteCapacity) {
        this.absoluteCapacity = absoluteCapacity;
        valueChanged("absoluteCapacity");
    }

    public double getAbsoluteMaxCapacity() {
        return absoluteMaxCapacity;
    }

    public void setAbsoluteMaxCapacity(double absoluteMaxCapacity) {
        this.absoluteMaxCapacity = absoluteMaxCapacity;
        valueChanged("absoluteMaxCapacity");
    }

    public double getAbsoluteUsedCapacity() {
        return absoluteUsedCapacity;
    }

    public void setAbsoluteUsedCapacity(double absoluteUsedCapacity) {
        this.absoluteUsedCapacity = absoluteUsedCapacity;
        valueChanged("absoluteUsedCapacity");
    }

    public long getMemory() {
        return memory;
    }

    public void setMemory(long memory) {
        this.memory = memory;
        valueChanged("memory");
    }

    public long getVcores() {
        return vcores;
    }

    public void setVcores(long vcores) {
        this.vcores = vcores;
        valueChanged("vcores");
    }

    public int getNumActiveApplications() {
        return numActiveApplications;
    }

    public void setNumActiveApplications(int numActiveApplications) {
        this.numActiveApplications = numActiveApplications;
        valueChanged("numActiveApplications");
    }

    public int getNumPendingApplications() {
        return numPendingApplications;
    }

    public void setNumPendingApplications(int numPendingApplications) {
        this.numPendingApplications = numPendingApplications;
        valueChanged("numPendingApplications");
    }

    public List<UserWrapper> getUsers() {
        return users;
    }

    public void setUsers(List<UserWrapper> users) {
        this.users = users;
        valueChanged("numPendingApplications");
    }
}
