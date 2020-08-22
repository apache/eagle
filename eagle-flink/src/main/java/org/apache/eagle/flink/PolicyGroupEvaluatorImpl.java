/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.eagle.flink;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PolicyGroupEvaluatorImpl implements PolicyGroupEvaluator {
    private static final long serialVersionUID = -5499413193675985288L;

    private static final Logger LOG = LoggerFactory.getLogger(PolicyGroupEvaluatorImpl.class);

    private AlertStreamCollector collector;
    // mapping from policy name to PolicyDefinition
    private volatile Map<String, PolicyDefinition> policyDefinitionMap = new HashMap<>();
    // mapping from policy name to PolicyStreamHandler
    private volatile Map<String, CompositePolicyHandler> policyStreamHandlerMap = new HashMap<>();
    private String policyEvaluatorId;
    private StreamContext context;

    public PolicyGroupEvaluatorImpl(String policyEvaluatorId) {
        this.policyEvaluatorId = policyEvaluatorId;
    }

    public void init(StreamContext context, AlertStreamCollector collector) {
        this.collector = collector;
        this.policyStreamHandlerMap = new HashMap<>();
        this.context = context;
        Thread.currentThread().setName(policyEvaluatorId);
    }

    public void nextEvent(PartitionedEvent event) {
        this.context.counter().incr("receive_count");
        dispatch(event);
    }

    @Override
    public String getName() {
        return this.policyEvaluatorId;
    }

    public void close() {
        for (PolicyStreamHandler handler : policyStreamHandlerMap.values()) {
            try {
                handler.close();
            } catch (Exception e) {
                LOG.error("Failed to close handler {}", handler.toString(), e);
            }
        }
    }

    /**
     * fixme make selection of PolicyStreamHandler to be more efficient.
     *
     * @param partitionedEvent PartitionedEvent
     */
    private void dispatch(PartitionedEvent partitionedEvent) {
        boolean handled = false;
        for (Map.Entry<String, CompositePolicyHandler> policyStreamHandler : policyStreamHandlerMap.entrySet()) {
            if (isAcceptedByPolicy(partitionedEvent, policyDefinitionMap.get(policyStreamHandler.getKey()))) {
                try {
                    handled = true;
                    this.context.counter().incr("eval_count");
                    policyStreamHandler.getValue().send(partitionedEvent.getEvent(), null);
                } catch (Exception e) {
                    this.context.counter().incr("fail_count");
                    LOG.error("{} failed to handle {}", policyStreamHandler.getValue(), partitionedEvent.getEvent(), e);
                }
            }
        }
        if (!handled) {
            this.context.counter().incr("drop_count");
            LOG.warn("Drop stream non-matched event {}, which should not be sent to evaluator", partitionedEvent);
        } else {
            this.context.counter().incr("accept_count");
        }
    }

    private static boolean isAcceptedByPolicy(PartitionedEvent event, PolicyDefinition policy) {
        return policy.getPartitionSpec().contains(event.getPartition())
            && (policy.getInputStreams().contains(event.getEvent().getStreamId())
            || policy.getDefinition().getInputStreams().contains(event.getEvent().getStreamId()));
    }


    @Override
    public void onPolicyChange(String version, List<PolicyDefinition> added, List<PolicyDefinition> removed, List<PolicyDefinition> modified, Map<String, StreamDefinition> sds) {
        Map<String, PolicyDefinition> copyPolicies = new HashMap<>(policyDefinitionMap);
        Map<String, CompositePolicyHandler> copyHandlers = new HashMap<>(policyStreamHandlerMap);
        for (PolicyDefinition pd : added) {
            inplaceAdd(copyPolicies, copyHandlers, pd, sds);
        }
        for (PolicyDefinition pd : removed) {
            inplaceRemove(copyPolicies, copyHandlers, pd);
        }
        for (PolicyDefinition pd : modified) {
            inplaceRemove(copyPolicies, copyHandlers, pd);
            inplaceAdd(copyPolicies, copyHandlers, pd, sds);
        }

        // logging
        LOG.info("{} with {} Policy metadata updated with added={}, removed={}, modified={}", policyEvaluatorId, version, added, removed, modified);

        // switch reference
        this.policyDefinitionMap = copyPolicies;
        this.policyStreamHandlerMap = copyHandlers;
    }

    private void inplaceAdd(Map<String, PolicyDefinition> policies, Map<String, CompositePolicyHandler> handlers, PolicyDefinition policy, Map<String, StreamDefinition> sds) {
        if (handlers.containsKey(policy.getName())) {
            LOG.error("metadata calculation error, try to add existing PolicyDefinition " + policy);
        } else {
            policies.put(policy.getName(), policy);
            CompositePolicyHandler handler = new CompositePolicyHandler(sds);
            try {
                PolicyHandlerContext handlerContext = new PolicyHandlerContext();
                handlerContext.setPolicyCounter(this.context.counter());
                handlerContext.setPolicyDefinition(policy);
                handlerContext.setPolicyEvaluator(this);
                handlerContext.setPolicyEvaluatorId(policyEvaluatorId);
                handlerContext.setConfig(this.context.config());
                handler.prepare(handlerContext);
                handlers.put(policy.getName(), handler);
            } catch (Exception e) {
                LOG.error(e.getMessage(), e);
                policies.remove(policy.getName());
                handlers.remove(policy.getName());
            }
        }
    }

    private void inplaceRemove(Map<String, PolicyDefinition> policies, Map<String, CompositePolicyHandler> handlers, PolicyDefinition policy) {
        if (handlers.containsKey(policy.getName())) {
            PolicyStreamHandler handler = handlers.get(policy.getName());
            try {
                handler.close();
            } catch (Exception e) {
                LOG.error("Failed to close handler {}", handler, e);
            } finally {
                policies.remove(policy.getName());
                handlers.remove(policy.getName());
                LOG.info("Removed policy: {}", policy);
            }
        } else {
            LOG.error("metadata calculation error, try to remove nonexisting PolicyDefinition: " + policy);
        }
    }


    public CompositePolicyHandler getPolicyHandler(String policy) {
        return policyStreamHandlerMap.get(policy);
    }

}