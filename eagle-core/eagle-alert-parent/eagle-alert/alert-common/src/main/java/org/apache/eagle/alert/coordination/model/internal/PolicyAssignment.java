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

/**
 * monitor metadata.
 *
 * @since Apr 27, 2016
 */
public class PolicyAssignment {

    private String version;

    private String policyName;
    private String queueId;

    public PolicyAssignment() {
    }

    public PolicyAssignment(String policyName, String queueId) {
        this.policyName = policyName;
        this.queueId = queueId;
    }

    public String getPolicyName() {
        return policyName;
    }

    public String getQueueId() {
        return queueId;
    }

    public void setPolicyName(String policyName) {
        this.policyName = policyName;
    }

    public void setQueueId(String queueId) {
        this.queueId = queueId;
    }

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    @Override
    public String toString() {
        return String.format("PolicyAssignment of policy %s, queueId %s, version %s !", policyName, queueId, version);
    }

}