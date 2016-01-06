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

package org.apache.eagle.executor;

/**
 * interface for reporting policy statistics including
 * 1. # of incoming events
 * 2. # of evaluated polices
 * 3. # of failures in policy evaluation
 * 4. # of alerts
 */
public interface PolicyStatsReporter {
    public final static String EAGLE_EVENT_COUNT = "eagle.event.count";
    public final static String EAGLE_POLICY_EVAL_COUNT = "eagle.policy.eval.count";
    public final static String EAGLE_POLICY_EVAL_FAIL_COUNT = "eagle.policy.eval.fail.count";
    public final static String EAGLE_ALERT_COUNT = "eagle.alert.count";
    void incrIncomingEvent();
    void incrPolicyEvaluation(String policyId);
    void incrPolicyEvaluationFailure(String policyId);
    void incrAlert(String policyId, int occurrences);
}
