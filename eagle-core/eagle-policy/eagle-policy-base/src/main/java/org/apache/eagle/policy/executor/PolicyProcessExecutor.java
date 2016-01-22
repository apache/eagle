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
package org.apache.eagle.policy.executor;

import com.codahale.metrics.MetricRegistry;
import com.sun.jersey.client.impl.CopyOnWriteHashMap;
import com.typesafe.config.Config;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.eagle.alert.entity.AbstractPolicyDefinitionEntity;
import org.apache.eagle.alert.entity.AlertDefinitionAPIEntity;
import org.apache.eagle.common.config.EagleConfigConstants;
import org.apache.eagle.dataproc.core.JsonSerDeserUtils;
import org.apache.eagle.dataproc.core.ValuesArray;
import org.apache.eagle.datastream.Collector;
import org.apache.eagle.datastream.JavaStormStreamExecutor2;
import org.apache.eagle.datastream.Tuple2;
import org.apache.eagle.metric.reportor.EagleCounterMetric;
import org.apache.eagle.metric.reportor.EagleMetricListener;
import org.apache.eagle.metric.reportor.EagleServiceReporterMetricListener;
import org.apache.eagle.metric.reportor.MetricKeyCodeDecoder;
import org.apache.eagle.policy.*;
import org.apache.eagle.policy.common.Constants;
import org.apache.eagle.policy.config.AbstractPolicyDefinition;
import org.apache.eagle.policy.dao.AlertStreamSchemaDAO;
import org.apache.eagle.policy.dao.AlertStreamSchemaDAOImpl;
import org.apache.eagle.policy.dao.PolicyDefinitionDAO;
import org.apache.eagle.policy.siddhi.SiddhiPolicyEvaluatorUtility;
import org.apache.eagle.policy.siddhi.StreamMetadataManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.management.ManagementFactory;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/**
 * The stream process executor based on two types
 * @since Dec 17, 2015
 *
 * @param <T> - The policy definition entity type
 * @param <K> - The stream entity type
 */
public abstract class PolicyProcessExecutor<T extends AbstractPolicyDefinitionEntity, K>
		extends JavaStormStreamExecutor2<String, K> 
		implements PolicyLifecycleMethods<T>, PolicyDistributionReportMethods, IPolicyExecutor<T, K>
{
	
	private static final long serialVersionUID = 1L;
	private static final Logger LOG = LoggerFactory.getLogger(PolicyProcessExecutor.class);
	
	public static final String EAGLE_EVENT_COUNT = "eagle.event.count";
	public static final String EAGLE_POLICY_EVAL_COUNT = "eagle.policy.eval.count";
	public static final String EAGLE_POLICY_EVAL_FAIL_COUNT = "eagle.policy.eval.fail.count";
	public static final String EAGLE_ALERT_COUNT = "eagle.alert.count";
	public static final String EAGLE_ALERT_FAIL_COUNT = "eagle.alert.fail.count";

	private	 static long MERITE_GRANULARITY = DateUtils.MILLIS_PER_MINUTE;

	private final Class<T> policyDefinitionClz;
	private String executorId;
	private volatile CopyOnWriteHashMap<String, PolicyEvaluator<T>> policyEvaluators;
	private PolicyPartitioner partitioner;
	private int numPartitions;
	private int partitionSeq;
	private Config config;
	private Map<String, Map<String, T>> initialAlertDefs;
	private String[] sourceStreams;
	private SiddhiPolicyEvaluatorUtility policyEvaluatorUtility = new SiddhiPolicyEvaluatorUtility();

	/**
	 * metricMap's key = metricName[#policyId]
	 */
	private Map<String, Map<String, String>> dimensionsMap; // cache it for performance
	private Map<String, String> baseDimensions;

	private MetricRegistry registry;
	private EagleMetricListener listener;

	private PolicyDefinitionDAO<T> policyDefinitionDao;

	public PolicyProcessExecutor(String alertExecutorId, PolicyPartitioner partitioner, int numPartitions, int partitionSeq,
                         PolicyDefinitionDAO<T> alertDefinitionDao, String[] sourceStreams, Class<T> clz){
		this.executorId = alertExecutorId;
		this.partitioner = partitioner;
		this.numPartitions = numPartitions;
		this.partitionSeq = partitionSeq;
		this.policyDefinitionDao = alertDefinitionDao;
		this.sourceStreams = sourceStreams;
		this.policyDefinitionClz = clz;
	}
	
	public String getExecutorId(){
		return this.executorId;
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
	
	public Map<String, Map<String, T>> getInitialAlertDefs() {
		return this.initialAlertDefs;
	}
		
	public PolicyDefinitionDAO<T> getPolicyDefinitionDao() {
		return policyDefinitionDao;
	}

    public Map<String, PolicyEvaluator<T>> getPolicyEvaluators(){
        return policyEvaluators;
    }
	
	@Override
	public void prepareConfig(Config config) {
		this.config = config;
	}
	
	private void initMetricReportor() {
		String host = config.getString(EagleConfigConstants.EAGLE_PROPS + "." + EagleConfigConstants.EAGLE_SERVICE + "." + EagleConfigConstants.HOST);
		int port = config.getInt(EagleConfigConstants.EAGLE_PROPS + "." + EagleConfigConstants.EAGLE_SERVICE + "." + EagleConfigConstants.PORT);

		String username = config.hasPath(EagleConfigConstants.EAGLE_PROPS + "." + EagleConfigConstants.EAGLE_SERVICE + "." + EagleConfigConstants.USERNAME) ?
				          config.getString(EagleConfigConstants.EAGLE_PROPS + "." + EagleConfigConstants.EAGLE_SERVICE + "." + EagleConfigConstants.USERNAME) : null;
		String password = config.hasPath(EagleConfigConstants.EAGLE_PROPS + "." + EagleConfigConstants.EAGLE_SERVICE + "." + EagleConfigConstants.PASSWORD) ?
				          config.getString(EagleConfigConstants.EAGLE_PROPS + "." + EagleConfigConstants.EAGLE_SERVICE + "." + EagleConfigConstants.PASSWORD) : null;
		
		registry = new MetricRegistry();
		listener = new EagleServiceReporterMetricListener(host, port, username, password);
		
		baseDimensions = new HashMap<>();
		baseDimensions = new HashMap<String, String>();
		baseDimensions.put(Constants.ALERT_EXECUTOR_ID, executorId);
		baseDimensions.put(Constants.PARTITIONSEQ, String.valueOf(partitionSeq));
		baseDimensions.put(Constants.SOURCE, ManagementFactory.getRuntimeMXBean().getName());
		baseDimensions.put(EagleConfigConstants.DATA_SOURCE, config.getString(EagleConfigConstants.EAGLE_PROPS + "." + EagleConfigConstants.DATA_SOURCE));
		baseDimensions.put(EagleConfigConstants.SITE, config.getString(EagleConfigConstants.EAGLE_PROPS + "." + EagleConfigConstants.SITE));

		dimensionsMap = new HashMap<String, Map<String, String>>();
	}

    public AlertStreamSchemaDAO getAlertStreamSchemaDAO(Config config){
        return new AlertStreamSchemaDAOImpl(config);
    }

	@Override
	public void init() {
		// initialize StreamMetadataManager before it is used
		StreamMetadataManager.getInstance().init(config, getAlertStreamSchemaDAO(config));
		// for each AlertDefinition, to create a PolicyEvaluator
		Map<String, PolicyEvaluator<T>> tmpPolicyEvaluators = new HashMap<String, PolicyEvaluator<T>>();
		
        String site = config.getString(EagleConfigConstants.EAGLE_PROPS + "." + EagleConfigConstants.SITE);
		String dataSource = config.getString(EagleConfigConstants.EAGLE_PROPS + "." + EagleConfigConstants.DATA_SOURCE);
		try {
			initialAlertDefs = policyDefinitionDao.findActivePoliciesGroupbyExecutorId(site, dataSource);
		}
		catch (Exception ex) {
			LOG.error("fail to initialize initialAlertDefs: ", ex);
            throw new IllegalStateException("fail to initialize initialAlertDefs: ", ex);
		}
        if(initialAlertDefs == null || initialAlertDefs.isEmpty()){
            LOG.warn("No alert definitions was found for site: " + site + ", dataSource: " + dataSource);
        }
        else if (initialAlertDefs.get(executorId) != null) { 
			for(T alertDef : initialAlertDefs.get(executorId).values()){
				int part = partitioner.partition(numPartitions, alertDef.getTags().get(Constants.POLICY_TYPE), alertDef.getTags().get(Constants.POLICY_ID));
				if (part == partitionSeq) {
					tmpPolicyEvaluators.put(alertDef.getTags().get(Constants.POLICY_ID), createPolicyEvaluator(alertDef));
				}
			}
		}
		
		policyEvaluators = new CopyOnWriteHashMap<>();
		// for efficiency, we don't put single policy evaluator
		policyEvaluators.putAll(tmpPolicyEvaluators);
		DynamicPolicyLoader<T> policyLoader = DynamicPolicyLoader.getInstanceOf(policyDefinitionClz);
		policyLoader.init(initialAlertDefs, policyDefinitionDao, config);
        String fullQualifiedAlertExecutorId = executorId + "_" + partitionSeq;
		policyLoader.addPolicyChangeListener(fullQualifiedAlertExecutorId, this);
        policyLoader.addPolicyDistributionReporter(fullQualifiedAlertExecutorId, this);
		LOG.info("Alert Executor created, partitionSeq: " + partitionSeq + " , numPartitions: " + numPartitions);
        LOG.info("All policy evaluators: " + policyEvaluators);
		
		initMetricReportor();
	}

    /**
     * Create PolicyEvaluator instance according to policyType-mapped policy evaluator class
     *
     * @param alertDef alert definition
     * @return PolicyEvaluator instance
     */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	protected PolicyEvaluator<T> createPolicyEvaluator(T alertDef){
		String policyType = alertDef.getTags().get(Constants.POLICY_TYPE);
		Class<? extends PolicyEvaluator> evalCls = PolicyManager.getInstance().getPolicyEvaluator(policyType);
		if(evalCls == null){
			String msg = "No policy evaluator defined for policy type : " + policyType;
			LOG.error(msg);
			throw new IllegalStateException(msg);
		}
		
		policyEvaluatorUtility.updateMarkdownDetails((AlertDefinitionAPIEntity) alertDef); // jira: EAGLE-95

		// check out whether strong incoming data validation is necessary
        String needValidationConfigKey= Constants.ALERT_EXECUTOR_CONFIGS + "." + executorId + ".needValidation";

        // Default: true
        boolean needValidation = !config.hasPath(needValidationConfigKey) || config.getBoolean(needValidationConfigKey);

		AbstractPolicyDefinition policyDef = null;
		try {
			policyDef = JsonSerDeserUtils.deserialize(alertDef.getPolicyDef(), AbstractPolicyDefinition.class, 
					PolicyManager.getInstance().getPolicyModules(policyType));
		} catch (Exception ex) {
			LOG.error("Fail initial alert policy def: "+alertDef.getPolicyDef(), ex);
		}
		PolicyEvaluator<T> pe;
		PolicyEvaluationContext<T, K> context = new PolicyEvaluationContext<>();
		context.policyId = alertDef.getTags().get("policyId");
		context.alertExecutor = this;
		context.resultRender = this.getResultRender();
		try {
			// create evaluator instance
			pe = (PolicyEvaluator<T>) evalCls
					.getConstructor(Config.class, PolicyEvaluationContext.class, AbstractPolicyDefinition.class, String[].class, boolean.class)
					.newInstance(config, context, policyDef, sourceStreams, needValidation);
		} catch(Exception ex) {
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
	private boolean accept(T alertDef){
		String executorID = alertDef.getTags().containsKey("executorId") ? alertDef.getTags().get("executorId")
				: alertDef.getTags().get("alertExecutorId");

		if(!executorID.equals(executorId)) {
            if(LOG.isDebugEnabled()){
                LOG.debug("alertDef does not belong to this alertExecutorId : " + executorId + ", alertDef : " + alertDef);
            }
            return false;
        }
		int targetPartitionSeq = partitioner.partition(numPartitions, alertDef.getTags().get(Constants.POLICY_TYPE), alertDef.getTags().get(Constants.POLICY_ID));
		if(targetPartitionSeq == partitionSeq)
			return true;
		return false;
	}

	private void updateCounter(String name, Map<String, String> dimensions, double value) {
		long current = System.currentTimeMillis();
		String metricName = MetricKeyCodeDecoder.codeMetricKey(name, dimensions);
		if (registry.getMetrics().get(metricName) == null) {
			EagleCounterMetric metric = new EagleCounterMetric(current, metricName, value, MERITE_GRANULARITY);
			metric.registerListener(listener);
			registry.register(metricName, metric);
		} else {
			EagleCounterMetric metric = (EagleCounterMetric) registry.getMetrics().get(metricName);
			metric.update(value, current);
			// TODO: need remove unused metric from registry
		}
	}
	
	private void updateCounter(String name, Map<String, String> dimensions) {
		updateCounter(name, dimensions, 1.0);
	}
	
	protected Map<String, String> getDimensions(String policyId) {
		if (dimensionsMap.get(policyId) == null) {
			Map<String, String> newDimensions = new HashMap<String, String>(baseDimensions);
			newDimensions.put(Constants.POLICY_ID, policyId);
			dimensionsMap.put(policyId, newDimensions);
		}
		return dimensionsMap.get(policyId);
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
    public void flatMap(java.util.List<Object> input, Collector<Tuple2<String, K>> outputCollector){
        if(input.size() != 3)
            throw new IllegalStateException("AlertExecutor always consumes exactly 3 fields: key, stream name and value(SortedMap)");
        if(LOG.isDebugEnabled()) LOG.debug("Msg is coming " + input.get(2));
        if(LOG.isDebugEnabled()) LOG.debug("Current policyEvaluators: " + policyEvaluators.keySet().toString());

        updateCounter(EAGLE_EVENT_COUNT, baseDimensions);
        try{
            synchronized(this.policyEvaluators) {
                for(Entry<String, PolicyEvaluator<T>> entry : policyEvaluators.entrySet()){
                    String policyId = entry.getKey();
                    PolicyEvaluator<T> evaluator = entry.getValue();
                    updateCounter(EAGLE_POLICY_EVAL_COUNT, getDimensions(policyId));
                    try {
                        evaluator.evaluate(new ValuesArray(outputCollector, input.get(1), input.get(2)));
                    }
                    catch (Exception ex) {
                        LOG.error("Got an exception, but continue to run " + input.get(2).toString(), ex);
                        updateCounter(EAGLE_POLICY_EVAL_COUNT, getDimensions(policyId));
                    }
                }
            }
        } catch(Exception ex){
            LOG.error(executorId + ", partition " + partitionSeq + ", error fetching alerts, but continue to run", ex);
            updateCounter(EAGLE_ALERT_FAIL_COUNT, baseDimensions);
        }
    }

	@Override
	public void onPolicyCreated(Map<String, T> added) {
		if(LOG.isDebugEnabled()) LOG.debug(executorId + ", partition " + partitionSeq + " policy added : " + added + " policyEvaluators " + policyEvaluators);
		for(T alertDef : added.values()){
			if(!accept(alertDef))
				continue;
			LOG.info(executorId + ", partition " + partitionSeq + " policy really added " + alertDef);
			PolicyEvaluator<T> newEvaluator = createPolicyEvaluator(alertDef);
			if(newEvaluator != null){
				synchronized(this.policyEvaluators) {
					policyEvaluators.put(alertDef.getTags().get(Constants.POLICY_ID), newEvaluator);
				}
			}
		}
	}

	@Override
	public void onPolicyChanged(Map<String, T> changed) {
		if(LOG.isDebugEnabled()) LOG.debug(executorId + ", partition " + partitionSeq + " policy changed : " + changed);
		for(T alertDef : changed.values()){
			if(!accept(alertDef))
				continue;
			policyEvaluatorUtility.updateMarkdownDetails((AlertDefinitionAPIEntity) alertDef); // jira: EAGLE-95
			LOG.info(executorId + ", partition " + partitionSeq + " policy really changed " + alertDef);
			synchronized(this.policyEvaluators) {
				PolicyEvaluator<T> pe = policyEvaluators.get(alertDef.getTags().get(Constants.POLICY_ID));
				pe.onPolicyUpdate(alertDef);
			}
		}
	}

	@Override
	public void onPolicyDeleted(Map<String, T> deleted) {
		if(LOG.isDebugEnabled()) LOG.debug(executorId + ", partition " + partitionSeq + " policy deleted : " + deleted);
		for(T alertDef : deleted.values()){
			if(!accept(alertDef))
				continue;
			LOG.info(executorId + ", partition " + partitionSeq + " policy really deleted " + alertDef);
			String policyId = alertDef.getTags().get(Constants.POLICY_ID);
			synchronized(this.policyEvaluators) {			
				if (policyEvaluators.containsKey(policyId)) {
					PolicyEvaluator<T> pe = policyEvaluators.remove(alertDef.getTags().get(Constants.POLICY_ID));
					pe.onPolicyDelete();
				}
			}
		}
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Override
	public void onEvalEvents(PolicyEvaluationContext<T, K> context, List<K> alerts) {
		if(alerts != null && !alerts.isEmpty()){
			String policyId = context.policyId;
            LOG.info(String.format("Detected %d alerts for policy %s", alerts.size(), policyId));
			Collector outputCollector = context.outputCollector;
			PolicyEvaluator<T> evaluator = context.evaluator;
			updateCounter(EAGLE_ALERT_COUNT, getDimensions(policyId), alerts.size());
			for (K entity : alerts) {
				synchronized(this) {
					outputCollector.collect(new Tuple2(policyId, entity));
				}
				if(LOG.isDebugEnabled()) LOG.debug("A new alert is triggered: " + executorId + ", partition " + partitionSeq + ", Got an alert with output context: " + entity + ", for policy " + evaluator);
			}
		}
	}

	public abstract ResultRender<T, K> getResultRender();

    @Override
    public void report() {
        PolicyDistroStatsLogReporter appender = new PolicyDistroStatsLogReporter();
        appender.reportPolicyMembership(executorId + "_" + partitionSeq, policyEvaluators.keySet());
    }
}
