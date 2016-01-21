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
package org.apache.eagle.policy.siddhi;

import com.typesafe.config.Config;
import org.apache.eagle.alert.entity.AbstractPolicyDefinitionEntity;
import org.apache.eagle.policy.PolicyEvaluationContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.stream.output.StreamCallback;

import java.util.LinkedList;
import java.util.List;

/**
 * Siddhi stream call back
 *
 * Created on 1/20/16.
 */
public class SiddhiOutputStreamCallback<T extends AbstractPolicyDefinitionEntity, K> extends StreamCallback {

    public static final Logger LOG = LoggerFactory.getLogger(SiddhiOutputStreamCallback.class);

    private SiddhiPolicyEvaluator<T, K> evaluator;
    public Config config;

    public SiddhiOutputStreamCallback(Config config, SiddhiPolicyEvaluator<T, K> evaluator) {
        this.config = config;
        this.evaluator = evaluator;
    }

    @Override
    public void receive(Event[] events) {
        long timeStamp = System.currentTimeMillis();
        List<K> alerts = new LinkedList<>();
        PolicyEvaluationContext<T, K> siddhiContext =  null;

        for (Event event : events) {
            Object[] data = event.getData();
            List<Object> returns = SiddhiQueryCallbackImpl.getOutputObject(event.getData());
            K alert = siddhiContext.resultRender.render(config, returns, siddhiContext, timeStamp);
            alerts.add(alert);

            if (siddhiContext == null) {
                siddhiContext = (PolicyEvaluationContext<T, K>) data[0];
            }
        }

        if (siddhiContext != null) {
            siddhiContext.alertExecutor.onEvalEvents(siddhiContext, alerts);
        }
    }
}
