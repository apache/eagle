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

import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.map.annotate.JsonSerialize;

@JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class Queue {
    //private String type;   workaround the YARN-4785,the field's value is based on the "type" field of SchedulerInfo.java, then its getter and setter function is never used.
    private double capacity;
    private double usedCapacity;
    private double maxCapacity;
    private double absoluteCapacity;
    private double absoluteMaxCapacity;
    private double absoluteUsedCapacity;

    private ResourcesUsed resourcesUsed;
    private String usedResources;
    private String queueName;
    private String state;
    private Users users;

    private int numApplications;
    private int numPendingApplications;
    private int numContainers;
    private int maxApplications;
    private int maxApplicationsPerUser;
    private int maxActiveApplications;
    private int maxActiveApplicationsPerUser;
    private int userLimit;
    private int userLimitFactor;
    private Queues queues;

    public String getUsedResources() {
        return usedResources;
    }

    public void setUsedResources(String usedResources) {
        this.usedResources = usedResources;
    }

    public int getMaxActiveApplicationsPerUser() {
        return maxActiveApplicationsPerUser;
    }

    public void setMaxActiveApplicationsPerUser(int maxActiveApplicationsPerUser) {
        this.maxActiveApplicationsPerUser = maxActiveApplicationsPerUser;
    }

    public int getNumPendingApplications() {
        return numPendingApplications;
    }

    public void setNumPendingApplications(int numPendingApplications) {
        this.numPendingApplications = numPendingApplications;
    }

    public int getNumContainers() {
        return numContainers;
    }

    public void setNumContainers(int numContainers) {
        this.numContainers = numContainers;
    }

    public int getMaxApplications() {
        return maxApplications;
    }

    public void setMaxApplications(int maxApplications) {
        this.maxApplications = maxApplications;
    }

    public int getMaxApplicationsPerUser() {
        return maxApplicationsPerUser;
    }

    public void setMaxApplicationsPerUser(int maxApplicationsPerUser) {
        this.maxApplicationsPerUser = maxApplicationsPerUser;
    }

    public int getMaxActiveApplications() {
        return maxActiveApplications;
    }

    public void setMaxActiveApplications(int maxActiveApplications) {
        this.maxActiveApplications = maxActiveApplications;
    }

    public int getUserLimit() {
        return userLimit;
    }

    public void setUserLimit(int userLimit) {
        this.userLimit = userLimit;
    }

    public int getUserLimitFactor() {
        return userLimitFactor;
    }

    public void setUserLimitFactor(int userLimitFactor) {
        this.userLimitFactor = userLimitFactor;
    }


    //    public String getType() {
    //        return type;
    //    }
    //
    //    public void setType(String type) {
    //        this.type = type;
    //    }

    public ResourcesUsed getResourcesUsed() {
        return resourcesUsed;
    }

    public void setResourcesUsed(ResourcesUsed resourcesUsed) {
        this.resourcesUsed = resourcesUsed;
    }


    public Users getUsers() {
        return users;
    }

    public void setUsers(Users users) {
        this.users = users;
    }

    public double getAbsoluteUsedCapacity() {
        return absoluteUsedCapacity;
    }

    public void setAbsoluteUsedCapacity(double absoluteUsedCapacity) {
        this.absoluteUsedCapacity = absoluteUsedCapacity;
    }

    public double getCapacity() {
        return capacity;
    }

    public void setCapacity(double capacity) {
        this.capacity = capacity;
    }

    public double getUsedCapacity() {
        return usedCapacity;
    }

    public void setUsedCapacity(double usedCapacity) {
        this.usedCapacity = usedCapacity;
    }

    public double getMaxCapacity() {
        return maxCapacity;
    }

    public void setMaxCapacity(double maxCapacity) {
        this.maxCapacity = maxCapacity;
    }

    public double getAbsoluteCapacity() {
        return absoluteCapacity;
    }

    public void setAbsoluteCapacity(double absoluteCapacity) {
        this.absoluteCapacity = absoluteCapacity;
    }

    public double getAbsoluteMaxCapacity() {
        return absoluteMaxCapacity;
    }

    public void setAbsoluteMaxCapacity(double absoluteMaxCapacity) {
        this.absoluteMaxCapacity = absoluteMaxCapacity;
    }

    public int getNumApplications() {
        return numApplications;
    }

    public void setNumApplications(int numApplications) {
        this.numApplications = numApplications;
    }

    public String getQueueName() {
        return queueName;
    }

    public void setQueueName(String queueName) {
        this.queueName = queueName;
    }

    public String getState() {
        return state;
    }

    public void setState(String state) {
        this.state = state;
    }

    public Queues getQueues() {
        return queues;
    }

    public void setQueues(Queues queues) {
        this.queues = queues;
    }
}
