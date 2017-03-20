/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.eagle.jpm.mr.runningentity;

import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import org.apache.eagle.log.base.taggedlog.TaggedLogAPIEntity;
import org.apache.eagle.log.entity.meta.*;

import static org.apache.eagle.jpm.util.Constants.ACCEPTED_APP_SERVICE_NAME;

@JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)
@Table("yarn_app")
@ColumnFamily("f")
@Prefix("accepted")
@Service(ACCEPTED_APP_SERVICE_NAME)
@TimeSeries(true)
@Partition( {"site"})
@Tags({"site","id","user","queue"})
public class YarnAppAPIEntity extends TaggedLogAPIEntity {
    @Column("a")
    private String appName;
    @Column("b")
    private String state;
    @Column("c")
    private long startedTime;
    @Column("d")
    private long elapsedTime;
    @Column("e")
    private String trackingUrl;
    @Column("f")
    private double queueUsagePercentage;
    @Column("g")
    private double clusterUsagePercentage;

    public String getAppName() {
        return appName;
    }

    public void setAppName(String appName) {
        this.appName = appName;
        valueChanged("appName");
    }

    public String getState() {
        return state;
    }

    public void setState(String state) {
        this.state = state;
        valueChanged("state");
    }

    public long getStartedTime() {
        return startedTime;
    }

    public void setStartedTime(long startedTime) {
        this.startedTime = startedTime;
        valueChanged("startedTime");
    }

    public long getElapsedTime() {
        return elapsedTime;
    }

    public void setElapsedTime(long elapsedTime) {
        this.elapsedTime = elapsedTime;
        valueChanged("elapsedTime");
    }

    public String getTrackingUrl() {
        return trackingUrl;
    }

    public void setTrackingUrl(String trackingUrl) {
        this.trackingUrl = trackingUrl;
        valueChanged("trackingUrl");
    }

    public double getQueueUsagePercentage() {
        return queueUsagePercentage;
    }

    public void setQueueUsagePercentage(double queueUsagePercentage) {
        this.queueUsagePercentage = queueUsagePercentage;
        valueChanged("queueUsagePercentage");
    }

    public double getClusterUsagePercentage() {
        return clusterUsagePercentage;
    }

    public void setClusterUsagePercentage(double clusterUsagePercentage) {
        this.clusterUsagePercentage = clusterUsagePercentage;
        valueChanged("clusterUsagePercentage");
    }
}
