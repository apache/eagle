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
package org.apache.eagle.alert.coordinator;

/**
 * A runtime option for one schedule processing.
 * 
 * Could used for configuration override.
 * 
 * @since Apr 19, 2016
 *
 */
public class ScheduleOption {
    private int policiesPerBolt;
    private int boltParallelism;
    private int policyDefaultParallelism;
    private double boltLoadUpbound;
    private double topoLoadUpbound;

    public int getPoliciesPerBolt() {
        return policiesPerBolt;
    }

    public void setPoliciesPerBolt(int policiesPerBolt) {
        this.policiesPerBolt = policiesPerBolt;
    }

    public int getBoltParallelism() {
        return boltParallelism;
    }

    public void setBoltParallelism(int boltParallelism) {
        this.boltParallelism = boltParallelism;
    }

    public int getPolicyDefaultParallelism() {
        return policyDefaultParallelism;
    }

    public void setPolicyDefaultParallelism(int policyDefaultParallelism) {
        this.policyDefaultParallelism = policyDefaultParallelism;
    }

    public double getBoltLoadUpbound() {
        return boltLoadUpbound;
    }

    public void setBoltLoadUpbound(double boltLoadUpbound) {
        this.boltLoadUpbound = boltLoadUpbound;
    }

    public double getTopoLoadUpbound() {
        return topoLoadUpbound;
    }

    public void setTopoLoadUpbound(double topoLoadUpbound) {
        this.topoLoadUpbound = topoLoadUpbound;
    }

}
