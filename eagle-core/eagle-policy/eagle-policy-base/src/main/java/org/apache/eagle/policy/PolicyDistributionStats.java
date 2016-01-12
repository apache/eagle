/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one or more
 *  * contributor license agreements.  See the NOTICE file distributed with
 *  * this work for additional information regarding copyright ownership.
 *  * The ASF licenses this file to You under the Apache License, Version 2.0
 *  * (the "License"); you may not use this file except in compliance with
 *  * the License.  You may obtain a copy of the License at
 *  *
 *  *    http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package org.apache.eagle.policy;

/**
 * fields for a policy distribution statistics
 */
public class PolicyDistributionStats {
    private String policyGroupId;   // normally groupId is alertExecutorId
    private String policyId;
    private boolean markDown;       // true if this policy is marked down, false otherwise
    private double weight;          // comprehensive factors for policy overhead

    public String getPolicyId() {
        return policyId;
    }

    public void setPolicyId(String policyId) {
        this.policyId = policyId;
    }

    public boolean isMarkDown() {
        return markDown;
    }

    public void setMarkDown(boolean markDown) {
        this.markDown = markDown;
    }

    public double getWeight() {
        return weight;
    }

    public void setWeight(double weight) {
        this.weight = weight;
    }

    public String getPolicyGroupId() {
        return policyGroupId;
    }

    public void setPolicyGroupId(String policyGroupId) {
        this.policyGroupId = policyGroupId;
    }

    public String toString(){
        StringBuilder sb = new StringBuilder();
        sb.append("policyId:");
        sb.append(policyId);
        sb.append(", markDown:");
        sb.append(markDown);
        sb.append(", weight:");
        sb.append(weight);

        return sb.toString();
    }
}
