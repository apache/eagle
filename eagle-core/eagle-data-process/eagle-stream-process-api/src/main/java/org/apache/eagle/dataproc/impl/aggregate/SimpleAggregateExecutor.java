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
package org.apache.eagle.dataproc.impl.aggregate;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.typesafe.config.Config;
import org.apache.eagle.dataproc.core.JsonSerDeserUtils;
import org.apache.eagle.dataproc.core.ValuesArray;
import org.apache.eagle.dataproc.impl.aggregate.entity.AggregateDefinitionAPIEntity;
import org.apache.eagle.dataproc.impl.aggregate.entity.AggregateEntity;
import org.apache.eagle.datastream.Collector;
import org.apache.eagle.datastream.JavaStormStreamExecutor2;
import org.apache.eagle.policy.PolicyEvaluationContext;
import org.apache.eagle.policy.PolicyEvaluator;
import org.apache.eagle.policy.PolicyManager;
import org.apache.eagle.policy.common.Constants;
import org.apache.eagle.policy.config.AbstractPolicyDefinition;
import org.apache.eagle.policy.executor.IPolicyExecutor;
import org.apache.eagle.policy.siddhi.SiddhiEvaluationHandler;
import org.apache.hadoop.hbase.util.MD5Hash;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Only one policy for one simple aggregate executor
 *
 * Created on 1/10/16.
 */
public class SimpleAggregateExecutor
        extends JavaStormStreamExecutor2<String, AggregateEntity>
        implements SiddhiEvaluationHandler<AggregateDefinitionAPIEntity, AggregateEntity>, IPolicyExecutor<AggregateDefinitionAPIEntity, AggregateEntity> {

    private static final Logger LOG = LoggerFactory.getLogger(SimpleAggregateExecutor.class);

    private final String cql;
    private final int partitionSeq;
    private final int totalPartitionNum;

    private final String[] upStreamNames;
    private String policyId;
    private String executorId;
    private Config config;
    private AggregateDefinitionAPIEntity aggDef;
    private PolicyEvaluator<AggregateDefinitionAPIEntity> evaluator;

    public SimpleAggregateExecutor(String[] upStreams, String cql, String policyType, int partitionSeq, int totalPartitionNum) {
        this.cql = cql;
        this.partitionSeq = partitionSeq;
        this.upStreamNames = upStreams;
        this.totalPartitionNum = totalPartitionNum;
        // create an fixed definition policy api entity, and indicate it has full definition
        aggDef = new AggregateDefinitionAPIEntity();
        aggDef.setTags(new HashMap<String, String>());
        aggDef.getTags().put(Constants.POLICY_TYPE, policyType);
        // TODO make it more general, not only hard code siddhi cep support here.
        try {
            Map<String,Object> template = new HashMap<>();
            template.put("type","siddhiCEPEngine");
            template.put("expression",this.cql);
            template.put("containsDefinition",true);
            aggDef.setPolicyDef(new ObjectMapper().writer().writeValueAsString(template));
        } catch (Exception e) {
            LOG.error("Simple aggregate generate policy definition failed!", e);
        }
        aggDef.setCreatedTime(new Date().getTime());
        aggDef.setLastModifiedDate(new Date().getTime());
        aggDef.setName("anonymous-aggregation-def");
        aggDef.setOwner("anonymous");
        aggDef.setEnabled(true);
        aggDef.setDescription("anonymous aggregation definition");

        String random = MD5Hash.getMD5AsHex(cql.getBytes());
        policyId = "anonymousAggregatePolicyId-" + random;
        executorId= "anonymousAggregateId-" +random;
    }

    @Override
    public void prepareConfig(Config config) {
        this.config = config;
    }

    @Override
    public void init() {
        evaluator = createPolicyEvaluator(aggDef);
    }

    /**
     * Create PolicyEvaluator instance according to policyType-mapped policy evaluator class
     *
     * @return PolicyEvaluator instance
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    protected PolicyEvaluator<AggregateDefinitionAPIEntity> createPolicyEvaluator(AggregateDefinitionAPIEntity alertDef) {
        String policyType = alertDef.getTags().get(Constants.POLICY_TYPE);
        Class<? extends PolicyEvaluator> evalCls = PolicyManager.getInstance().getPolicyEvaluator(policyType);
        if (evalCls == null) {
            String msg = "No policy evaluator defined for policy type : " + policyType;
            LOG.error(msg);
            throw new IllegalStateException(msg);
        }

        AbstractPolicyDefinition policyDef = null;
        try {
            policyDef = JsonSerDeserUtils.deserialize(alertDef.getPolicyDef(), AbstractPolicyDefinition.class,
                    PolicyManager.getInstance().getPolicyModules(policyType));
        } catch (Exception ex) {
            LOG.error("Fail initial alert policy def: " + alertDef.getPolicyDef(), ex);
        }

        PolicyEvaluator<AggregateDefinitionAPIEntity> pe;
        PolicyEvaluationContext<AggregateDefinitionAPIEntity, AggregateEntity> context = new PolicyEvaluationContext<>();
        context.policyId = alertDef.getTags().get("policyId");
        context.alertExecutor = this;
        context.resultRender = new AggregateResultRender();
        try {
            // create evaluator instances
            pe = (PolicyEvaluator<AggregateDefinitionAPIEntity>) evalCls
                    .getConstructor(Config.class, PolicyEvaluationContext.class, AbstractPolicyDefinition.class, String[].class, boolean.class)
                    .newInstance(config, context, policyDef, upStreamNames, false);
        } catch (Exception ex) {
            LOG.error("Fail creating new policyEvaluator", ex);
            LOG.warn("Broken policy definition and stop running : " + alertDef.getPolicyDef());
            throw new IllegalStateException(ex);
        }
        return pe;
    }

    @Override
    public void flatMap(List<Object> input, Collector<Tuple2<String, AggregateEntity>> collector) {
        if (input.size() != 3)
            throw new IllegalStateException("AggregateExecutor always consumes exactly 3 fields: key, stream name and value(SortedMap)");
        if (LOG.isDebugEnabled()) LOG.debug("Msg is coming " + input.get(2));
        if (LOG.isDebugEnabled()) LOG.debug("Current policyEvaluators: " + evaluator);

        try {
            evaluator.evaluate(new ValuesArray(collector, input.get(1), input.get(2)));
        } catch (Exception ex) {
            LOG.error("Got an exception, but continue to run " + input.get(2).toString(), ex);
        }
    }

    @Override
    public void onEvalEvents(PolicyEvaluationContext<AggregateDefinitionAPIEntity, AggregateEntity> context, List<AggregateEntity> alerts) {
        if (alerts != null && !alerts.isEmpty()) {
            String policyId = context.policyId;
            LOG.info(String.format("Detected %d alerts for policy %s", alerts.size(), policyId));
            Collector outputCollector = context.outputCollector;
            PolicyEvaluator<AggregateDefinitionAPIEntity> evaluator = context.evaluator;
            for (AggregateEntity entity : alerts) {
                synchronized (this) {
                    outputCollector.collect(new Tuple2(policyId, entity));
                }
                if (LOG.isDebugEnabled()) {
                    LOG.debug("A new alert is triggered: " + executorId + ", partition " + partitionSeq + ", Got an alert with output context: " + entity + ", for policy " + evaluator);
                }
            }
        }
    }

    @Override
    public String getExecutorId() {
        return executorId;
    }

    @Override
    public int getPartitionSeq() {
        return partitionSeq;
    }
}
