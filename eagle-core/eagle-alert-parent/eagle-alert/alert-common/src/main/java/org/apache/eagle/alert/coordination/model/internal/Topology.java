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

import java.util.HashSet;
import java.util.Set;

/**
 * Logically one unit topology consists of S spouts, G
 * groupby bolts, A alertBolts normally S=1 Physically each spout is
 * composed of s spout nodes, each groupby bolt is composed of g groupby
 * nodes, and each alert bolt is composed of a alert nodes.
 *
 * @since Mar 24, 2016
 */
public class Topology {

    private String name;
    // number of logical nodes
    private int numOfSpout;
    private int numOfAlertBolt;
    private int numOfGroupBolt;
    private int numOfPublishBolt;
    private String spoutId;
    private String pubBoltId;
    @Deprecated
    private Set<String> groupNodeIds;
    @Deprecated
    private Set<String> alertBoltIds;

    // number of physical nodes for each logic bolt
    private int spoutParallelism = 1;
    private int groupParallelism = 1;
    private int alertParallelism = 1;

    private String clusterName;

    public Topology() {
    }

    public Topology(String name, int group, int alert) {
        this.name = name;
        this.numOfSpout = 1;
        this.numOfGroupBolt = group;
        this.numOfAlertBolt = alert;
        groupNodeIds = new HashSet<String>(group);
        alertBoltIds = new HashSet<String>(alert);

        spoutParallelism = 1;
        groupParallelism = 1;
        alertParallelism = 1;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getNumOfSpout() {
        return numOfSpout;
    }

    public void setNumOfSpout(int numOfSpout) {
        this.numOfSpout = numOfSpout;
    }

    public int getNumOfAlertBolt() {
        return numOfAlertBolt;
    }

    public void setNumOfAlertBolt(int numOfAlertBolt) {
        this.numOfAlertBolt = numOfAlertBolt;
    }

    public int getNumOfGroupBolt() {
        return numOfGroupBolt;
    }

    public void setNumOfGroupBolt(int numOfGroupBolt) {
        this.numOfGroupBolt = numOfGroupBolt;
    }

    public String getSpoutId() {
        return spoutId;
    }

    public void setSpoutId(String spoutId) {
        this.spoutId = spoutId;
    }

    public String getPubBoltId() {
        return pubBoltId;
    }

    public void setPubBoltId(String pubBoltId) {
        this.pubBoltId = pubBoltId;
    }

    public Set<String> getGroupNodeIds() {
        return groupNodeIds;
    }

    public void setGroupNodeIds(Set<String> groupNodeIds) {
        this.groupNodeIds = groupNodeIds;
    }

    public Set<String> getAlertBoltIds() {
        return alertBoltIds;
    }

    public void setAlertBoltIds(Set<String> alertBoltIds) {
        this.alertBoltIds = alertBoltIds;
    }

    public int getNumOfPublishBolt() {
        return numOfPublishBolt;
    }

    public void setNumOfPublishBolt(int numOfPublishBolt) {
        this.numOfPublishBolt = numOfPublishBolt;
    }

    public int getSpoutParallelism() {
        return spoutParallelism;
    }

    public void setSpoutParallelism(int spoutParallelism) {
        this.spoutParallelism = spoutParallelism;
    }

    public int getGroupParallelism() {
        return groupParallelism;
    }

    public void setGroupParallelism(int groupParallelism) {
        this.groupParallelism = groupParallelism;
    }

    public int getAlertParallelism() {
        return alertParallelism;
    }

    public void setAlertParallelism(int alertParallelism) {
        this.alertParallelism = alertParallelism;
    }

    public String getClusterName() {
        return clusterName;
    }

    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
    }

}
