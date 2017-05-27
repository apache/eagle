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

package org.apache.eagle.jpm.mr.historyentity;

import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import org.apache.eagle.log.base.taggedlog.TaggedLogAPIEntity;
import org.apache.eagle.log.entity.meta.*;

import static org.apache.eagle.jpm.util.Constants.MR_JOB_RPC_ANALYSIS_SERVICE_NAME;

@JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)
@Table("eaglejpa_analysis")
@ColumnFamily("f")
@Prefix("counter")
@Service(MR_JOB_RPC_ANALYSIS_SERVICE_NAME)
@TimeSeries(true)
@Partition({"site"})
@Indexes({
        @Index(name = "Index_1_jobId", columns = { "jobId" }, unique = true),
        @Index(name = "Index_2_jobDefId", columns = { "jobDefId" }, unique = false)
        })
public class JobRpcAnalysisAPIEntity extends TaggedLogAPIEntity {
    @Column("a")
    private String trackingUrl;
    @Column("b")
    private String currentState;
    @Column("c")
    private double totalOpsPerSecond;
    @Column("d")
    private double mapOpsPerSecond;
    @Column("e")
    private double reduceOpsPerSecond;
    @Column("f")
    private double avgOpsPerTask;
    @Column("g")
    private double avgOpsPerMap;
    @Column("h")
    private double avgOpsPerReduce;
    @Column("i")
    private double avgMapTime;
    @Column("j")
    private double avgReduceTime;
    @Column("k")
    private int numTotalMaps;
    @Column("l")
    private int numTotalReduces;
    @Column("m")
    private long duration;

    public String getTrackingUrl() {
        return trackingUrl;
    }

    public void setTrackingUrl(String trackingUrl) {
        this.trackingUrl = trackingUrl;
        valueChanged("trackingUrl");
    }

    public String getCurrentState() {
        return currentState;
    }

    public void setCurrentState(String currentState) {
        this.currentState = currentState;
        valueChanged("currentState");
    }

    public double getTotalOpsPerSecond() {
        return totalOpsPerSecond;
    }

    public void setTotalOpsPerSecond(double totalOpsPerSecond) {
        this.totalOpsPerSecond = totalOpsPerSecond;
        valueChanged("totalOpsPerSecond");
    }

    public double getMapOpsPerSecond() {
        return mapOpsPerSecond;
    }

    public void setMapOpsPerSecond(double mapOpsPerSecond) {
        this.mapOpsPerSecond = mapOpsPerSecond;
        valueChanged("mapOpsPerSecond");
    }

    public double getReduceOpsPerSecond() {
        return reduceOpsPerSecond;
    }

    public void setReduceOpsPerSecond(double reduceOpsPerSecond) {
        this.reduceOpsPerSecond = reduceOpsPerSecond;
        valueChanged("reduceOpsPerSecond");
    }

    public double getAvgOpsPerTask() {
        return avgOpsPerTask;
    }

    public void setAvgOpsPerTask(double avgOpsPerTask) {
        this.avgOpsPerTask = avgOpsPerTask;
        valueChanged("avgOpsPerTask");
    }

    public double getAvgOpsPerMap() {
        return avgOpsPerMap;
    }

    public void setAvgOpsPerMap(double avgOpsPerMap) {
        this.avgOpsPerMap = avgOpsPerMap;
        valueChanged("avgOpsPerMap");
    }

    public double getAvgOpsPerReduce() {
        return avgOpsPerReduce;
    }

    public void setAvgOpsPerReduce(double avgOpsPerReduce) {
        this.avgOpsPerReduce = avgOpsPerReduce;
        valueChanged("avgOpsPerReduce");
    }

    public double getAvgMapTime() {
        return avgMapTime;
    }

    public void setAvgMapTime(double avgMapTime) {
        this.avgMapTime = avgMapTime;
        valueChanged("avgMapTime");
    }

    public double getAvgReduceTime() {
        return avgReduceTime;
    }

    public void setAvgReduceTime(double avgReduceTime) {
        this.avgReduceTime = avgReduceTime;
        valueChanged("avgReduceTime");
    }

    public int getNumTotalMaps() {
        return numTotalMaps;
    }

    public void setNumTotalMaps(int numTotalMaps) {
        this.numTotalMaps = numTotalMaps;
        valueChanged("numTotalMaps");
    }

    public int getNumTotalReduces() {
        return numTotalReduces;
    }

    public void setNumTotalReduces(int numTotalReduces) {
        this.numTotalReduces = numTotalReduces;
        valueChanged("numTotalReduces");
    }

    public long getDuration() {
        return duration;
    }

    public void setDuration(long duration) {
        this.duration = duration;
        valueChanged("duration");
    }

}

