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

public class UserWrapper {

    private String username;
    private long memory;
    private long vCores;
    private int numPendingApplications;
    private int numActiveApplications;

    public UserWrapper(User user) {
        this.username = user.getUsername();
        this.memory = user.getResourcesUsed().getMemory();
        this.vCores = user.getResourcesUsed().getvCores();
        this.numActiveApplications = user.getNumActiveApplications();
        this.numPendingApplications = user.getNumPendingApplications();
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public long getMemory() {
        return memory;
    }

    public void setMemory(long memory) {
        this.memory = memory;
    }

    public long getvCores() {
        return vCores;
    }

    public void setvCores(long vCores) {
        this.vCores = vCores;
    }

    public int getNumPendingApplications() {
        return numPendingApplications;
    }

    public void setNumPendingApplications(int numPendingApplications) {
        this.numPendingApplications = numPendingApplications;
    }

    public int getNumActiveApplications() {
        return numActiveApplications;
    }

    public void setNumActiveApplications(int numActiveApplications) {
        this.numActiveApplications = numActiveApplications;
    }
}
