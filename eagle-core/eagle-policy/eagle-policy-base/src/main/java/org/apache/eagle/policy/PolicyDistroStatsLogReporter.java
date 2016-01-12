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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;

/**
 * just append log
 */
public class PolicyDistroStatsLogReporter implements PolicyDistributionStatsDAO{
    private static Logger LOG = LoggerFactory.getLogger(PolicyDistroStatsLogReporter.class);

    @Override
    public void reportPolicyMembership(String policyGroupId, Set<String> policyIds) {
        if(policyIds != null){
            StringBuilder sb = new StringBuilder();
            sb.append("policyDistributionStats for " + policyGroupId +", total: " + policyIds.size() + ", [");
            for(String policyId : policyIds){
                sb.append(policyId + ",");
            }
            if(policyIds.size() > 0){
                sb.deleteCharAt(sb.length()-1);
            }
            sb.append("]");
            LOG.info(sb.toString());
        }else{
            LOG.warn("No policies are assigned to " + policyGroupId);
        }
    }
}
