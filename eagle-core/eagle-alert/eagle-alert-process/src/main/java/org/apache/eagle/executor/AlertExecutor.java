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
package org.apache.eagle.executor;

import com.sun.jersey.client.impl.CopyOnWriteHashMap;
import com.typesafe.config.Config;
import org.apache.eagle.alert.common.AlertConstants;
import org.apache.eagle.alert.config.AbstractPolicyDefinition;
import org.apache.eagle.alert.dao.AlertDefinitionDAO;
import org.apache.eagle.alert.dao.AlertStreamSchemaDAO;
import org.apache.eagle.alert.dao.AlertStreamSchemaDAOImpl;
import org.apache.eagle.alert.entity.AlertAPIEntity;
import org.apache.eagle.alert.entity.AlertDefinitionAPIEntity;
import org.apache.eagle.alert.policy.*;
import org.apache.eagle.state.base.Snapshotable;
import org.apache.eagle.state.snapshot.ByteSerializer;
import org.apache.eagle.alert.siddhi.EagleAlertContext;
import org.apache.eagle.alert.siddhi.SiddhiAlertHandler;
import org.apache.eagle.alert.siddhi.StreamMetadataManager;
import org.apache.eagle.common.config.EagleConfigConstants;
import org.apache.eagle.dataproc.core.JsonSerDeserUtils;
import org.apache.eagle.dataproc.core.ValuesArray;
import org.apache.eagle.datastream.Collector;
import org.apache.eagle.datastream.JavaStormStreamExecutor2;
import org.apache.eagle.datastream.Tuple2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

public class AlertExecutor extends JavaStormStreamExecutor2<String, AlertAPIEntity> implements PolicyLifecycleMethods, SiddhiAlertHandler,PolicyDistributionReportMethods, Snapshotable {
    private static final Logger LOG = LoggerFactory.getLogger(AlertExecutor.class);

	private String alertExecutorId;
	private volatile CopyOnWriteHashMap<String, PolicyEvaluator> policyEvaluators;
	private PolicyPartitioner partitioner;
	private int numPartitions;
	private int partitionSeq;
	private Config config;
	private Map<String, Map<String, AlertDefinitionAPIEntity>> initialAlertDefs;
	private AlertDefinitionDAO alertDefinitionDao;
	private String[] sourceStreams;

    private PolicyStatsReporter reporter;

	public AlertExecutor(String alertExecutorId, PolicyPartitioner partitioner, int numPartitions, int partitionSeq,
                         AlertDefinitionDAO alertDefinitionDao, String[] sourceStreams){
		this.alertExecutorId = alertExecutorId;
		this.partitioner = partitioner;
		this.numPartitions = numPartitions;
		this.partitionSeq = partitionSeq;
		this.alertDefinitionDao = alertDefinitionDao;
		this.sourceStreams = sourceStreams;
	}
	
	public String getAlertExecutorId(){
		return this.alertExecutorId;
	}

	public int getNumPartitions() {
		return this.numPartitions;
	}
	
	public int getPartitionSeq(){
		return this.partitionSeq;
	}
	
	public PolicyPartitioner getPolicyPartitioner() {
		return this.partitioner;
	}
	
	public Map<String, Map<String, AlertDefinitionAPIEntity>> getInitialAlertDefs() {
		return this.initialAlertDefs;
	}
		
	public AlertDefinitionDAO getAlertDefinitionDao() {
		return alertDefinitionDao;
	}

    public Map<String, PolicyEvaluator> getPolicyEvaluators(){
        return policyEvaluators;
    }
	
	@Override
	public void prepareConfig(Config config) {
		this.config = config;
	}
	
    /**
     * for unit test purpose only
     * @param config
     * @return
     */
    public AlertStreamSchemaDAO getAlertStreamSchemaDAO(Config config){
        return new AlertStreamSchemaDAOImpl(config);
    }

	@Override
	public void init() {
		// initialize StreamMetadataManager before it is used
		StreamMetadataManager.getInstance().init(config, getAlertStreamSchemaDAO(config));
		// for each AlertDefinition, to create a PolicyEvaluator
		Map<String, PolicyEvaluator> tmpPolicyEvaluators = new HashMap<String, PolicyEvaluator>();
		
        String site = config.getString(EagleConfigConstants.EAGLE_PROPS + "." + EagleConfigConstants.SITE);
		String dataSource = config.getString(EagleConfigConstants.EAGLE_PROPS + "." + EagleConfigConstants.DATA_SOURCE);
		try {
			initialAlertDefs = alertDefinitionDao.findActiveAlertDefsGroupbyAlertExecutorId(site, dataSource);
		}
		catch (Exception ex) {
			LOG.error("fail to initialize initialAlertDefs: ", ex);
            throw new IllegalStateException("fail to initialize initialAlertDefs: ", ex);
		}
        if(initialAlertDefs == null || initialAlertDefs.isEmpty()){
            LOG.warn("No alert definitions was found for site: " + site + ", dataSource: " + dataSource);
        }
        else if (initialAlertDefs.get(alertExecutorId) != null) { 
			for(AlertDefinitionAPIEntity alertDef : initialAlertDefs.get(alertExecutorId).values()){
				int part = partitioner.partition(numPartitions, alertDef.getTags().get(AlertConstants.POLICY_TYPE), alertDef.getTags().get(AlertConstants.POLICY_ID));
				if (part == partitionSeq) {
					tmpPolicyEvaluators.put(alertDef.getTags().get(AlertConstants.POLICY_ID), createPolicyEvaluator(alertDef));
				}
			}
		}
		
		policyEvaluators = new CopyOnWriteHashMap<>();
		// for efficiency, we don't put single policy evaluator
		policyEvaluators.putAll(tmpPolicyEvaluators);
		DynamicPolicyLoader policyLoader = DynamicPolicyLoader.getInstance();
		
		policyLoader.init(initialAlertDefs, alertDefinitionDao, config);
        String fullQualifiedAlertExecutorId = alertExecutorId + "_" + partitionSeq;
		policyLoader.addPolicyChangeListener(fullQualifiedAlertExecutorId, this);
        policyLoader.addPolicyDistributionReporter(fullQualifiedAlertExecutorId, this);
		LOG.info("Alert Executor created, partitionSeq: " + partitionSeq + " , numPartitions: " + numPartitions);
        LOG.info("All policy evaluators: " + policyEvaluators);
		
        reporter = PolicyStatsReporters.newReporter(config, alertExecutorId, partitionSeq);
	}

    /**
     * Create PolicyEvaluator instance according to policyType-mapped policy evaluator class
     *
     * @param alertDef alert definition
     * @return PolicyEvaluator instance
     */
	private PolicyEvaluator createPolicyEvaluator(AlertDefinitionAPIEntity alertDef){
		String policyType = alertDef.getTags().get(AlertConstants.POLICY_TYPE);
		Class<? extends PolicyEvaluator> evalCls = PolicyManager.getInstance().getPolicyEvaluator(policyType);
		if(evalCls == null){
			String msg = "No policy evaluator defined for policy type : " + policyType;
			LOG.error(msg);
			throw new IllegalStateException(msg);
		}
		
		// check out whether strong incoming data validation is necessary
        String needValidationConfigKey= AlertConstants.ALERT_EXECUTOR_CONFIGS + "." + alertExecutorId + ".needValidation";

        // Default: true
        boolean needValidation = !config.hasPath(needValidationConfigKey) || config.getBoolean(needValidationConfigKey);

		AbstractPolicyDefinition policyDef = null;
		try {
			policyDef = JsonSerDeserUtils.deserialize(alertDef.getPolicyDef(), AbstractPolicyDefinition.class, PolicyManager.getInstance().getPolicyModules(policyType));
		} catch (Exception ex) {
			LOG.error("Fail initial alert policy def: "+alertDef.getPolicyDef(), ex);
		}
		PolicyEvaluator pe;
		try{
            // Create evaluator instances
			pe = evalCls.getConstructor(Config.class, String.class, AbstractPolicyDefinition.class, String[].class, boolean.class).newInstance(config, alertDef.getTags().get("policyId"), policyDef, sourceStreams, needValidation);
		}catch(Exception ex){
			LOG.error("Fail creating new policyEvaluator", ex);
			LOG.warn("Broken policy definition and stop running : " + alertDef.getPolicyDef());
			throw new IllegalStateException(ex);
		}
		return pe;
	}

    /**
     * verify both alertExecutor logic name and partition id
     * @param alertDef alert definition
     *
     * @return whether accept the alert definition
     */
	private boolean accept(AlertDefinitionAPIEntity alertDef){
        if(!alertDef.getTags().get("alertExecutorId").equals(alertExecutorId)) {
            if(LOG.isDebugEnabled()){
                LOG.debug("alertDef does not belong to this alertExecutorId : " + alertExecutorId + ", alertDef : " + alertDef);
            }
            return false;
        }
		int targetPartitionSeq = partitioner.partition(numPartitions, alertDef.getTags().get(AlertConstants.POLICY_TYPE), alertDef.getTags().get(AlertConstants.POLICY_ID));
		if(targetPartitionSeq == partitionSeq)
			return true;
		return false;
	}
	
	public long trim(long value, long granularity) {
		return value / granularity * granularity;
	}

    /**
     * within this single executor, execute all PolicyEvaluator sequentially
     * the contract for input:
     * 1. total # of fields for input is 3, which is fixed
     * 2. the first field is key
     * 3. the second field is stream name
     * 4. the third field is value which is java SortedMap
     */
    @Override
    public void flatMap(java.util.List<Object> input, Collector<Tuple2<String, AlertAPIEntity>> outputCollector){
        if(input.size() != 3)
            throw new IllegalStateException("AlertExecutor always consumes exactly 3 fields: key, stream name and value(SortedMap)");
        if(LOG.isDebugEnabled()) LOG.debug("Msg is coming " + input.get(2));
        if(LOG.isDebugEnabled()) LOG.debug("Current policyEvaluators: " + policyEvaluators.keySet().toString());

        reporter.incrIncomingEvent();
        synchronized(this.policyEvaluators) {
            for(Entry<String, PolicyEvaluator> entry : policyEvaluators.entrySet()){
                String policyId = entry.getKey();
                PolicyEvaluator evaluator = entry.getValue();
                reporter.incrPolicyEvaluation(policyId);
                try {
                    EagleAlertContext siddhiAlertContext = new EagleAlertContext();
                    siddhiAlertContext.alertExecutor = this;
                    siddhiAlertContext.policyId = policyId;
                    siddhiAlertContext.evaluator = evaluator;
                    siddhiAlertContext.outputCollector = outputCollector;
                    evaluator.evaluate(new ValuesArray(siddhiAlertContext, input.get(1), input.get(2)));
                }
                catch (Exception ex) {
                    LOG.error("Error evaluating policy, but continue to run for " + alertExecutorId + ", partition " + partitionSeq + ", with event " + input.get(2).toString(), ex);
                    reporter.incrPolicyEvaluationFailure(policyId);
                }
            }
        }
    }

	@Override
	public void onPolicyCreated(Map<String, AlertDefinitionAPIEntity> added) {
		if(LOG.isDebugEnabled()) LOG.debug(alertExecutorId + ", partition " + partitionSeq + " policy added : " + added + " policyEvaluators " + policyEvaluators);
		for(AlertDefinitionAPIEntity alertDef : added.values()){
			if(!accept(alertDef))
				continue;
			LOG.info(alertExecutorId + ", partition " + partitionSeq + " policy really added " + alertDef);
			PolicyEvaluator newEvaluator = createPolicyEvaluator(alertDef);
			if(newEvaluator != null){
				synchronized(this.policyEvaluators) {
					policyEvaluators.put(alertDef.getTags().get(AlertConstants.POLICY_ID), newEvaluator);
				}
			}
		}
	}

	@Override
	public void onPolicyChanged(Map<String, AlertDefinitionAPIEntity> changed) {
		if(LOG.isDebugEnabled()) LOG.debug(alertExecutorId + ", partition " + partitionSeq + " policy changed : " + changed);
		for(AlertDefinitionAPIEntity alertDef : changed.values()){
			if(!accept(alertDef))
				continue;
			LOG.info(alertExecutorId + ", partition " + partitionSeq + " policy really changed " + alertDef);
			synchronized(this.policyEvaluators) {
				PolicyEvaluator pe = policyEvaluators.get(alertDef.getTags().get(AlertConstants.POLICY_ID));
				pe.onPolicyUpdate(alertDef);
			}
		}
	}

	@Override
	public void onPolicyDeleted(Map<String, AlertDefinitionAPIEntity> deleted) {
		if(LOG.isDebugEnabled()) LOG.debug(alertExecutorId + ", partition " + partitionSeq + " policy deleted : " + deleted);
		for(AlertDefinitionAPIEntity alertDef : deleted.values()){
			if(!accept(alertDef))
				continue;
			LOG.info(alertExecutorId + ", partition " + partitionSeq + " policy really deleted " + alertDef);
			String policyId = alertDef.getTags().get(AlertConstants.POLICY_ID);
			synchronized(this.policyEvaluators) {			
				if (policyEvaluators.containsKey(policyId)) {
					PolicyEvaluator pe = policyEvaluators.remove(alertDef.getTags().get(AlertConstants.POLICY_ID));
					pe.onPolicyDelete();
				}
			}
		}
	}

    /**
     * This is probably called in another thread other than the thread where event is sent.
     * collect method is synchronized to make sure asynchronized callback consistent
     * @param context
     * @param alerts
     */
	@Override
	public void onAlerts(EagleAlertContext context, List<AlertAPIEntity> alerts) {
		if(alerts != null && !alerts.isEmpty()){
			String policyId = context.policyId;
            LOG.info(String.format("Detected %s alerts for policy %s",alerts.size(),policyId));
			Collector outputCollector = context.outputCollector;
			PolicyEvaluator evaluator = context.evaluator;
            reporter.incrAlert(policyId, alerts.size());
			for (AlertAPIEntity entity : alerts) {
				synchronized(this) {
					outputCollector.collect(new Tuple2(policyId, entity));
				}
				if(LOG.isDebugEnabled()) LOG.debug("A new alert is triggered: "+alertExecutorId + ", partition " + partitionSeq + ", Got an alert with output context: " + entity.getAlertContext() + ", for policy " + evaluator);
			}
		}
	}

    @Override
    public void report() {
        PolicyDistroStatsLogReporter appender = new PolicyDistroStatsLogReporter();
        appender.reportPolicyMembership(alertExecutorId + "_" + partitionSeq, policyEvaluators.keySet());
    }

    /**
     * this is stop-the-world operation
     * @return
     */
    @Override
    public byte[] currentState() {
        Map<String, byte[]> map = new HashMap<String, byte[]>();
        synchronized (this.policyEvaluators) {
            for(PolicyEvaluator evaluator : this.policyEvaluators.values()){
                if(evaluator instanceof Snapshotable){
                    Snapshotable snapshotable = (Snapshotable)evaluator;
                    map.put(snapshotable.getElementId(), snapshotable.currentState());
                }
            }
        }
        // serialization need not be synchronized
        return ByteSerializer.OToB(map);
    }

    @Override
    public void restoreState(byte[] state) {
        Map<String, byte[]> map = (Map<String, byte[]>)ByteSerializer.BToO(state);
        if(map == null)
            return;
        for(String policyId : map.keySet()){
            PolicyEvaluator evaluator = policyEvaluators.get(policyId);
            if(evaluator != null && evaluator instanceof Snapshotable){
                Snapshotable snapshotable = (Snapshotable)evaluator;
                snapshotable.restoreState(map.get(policyId));
            }else{
                LOG.info("restoring non existing policy " + policyId);
            }
        }
    }

    @Override
    public String getElementId() {
        return alertExecutorId + "_" + partitionSeq;
    }
}