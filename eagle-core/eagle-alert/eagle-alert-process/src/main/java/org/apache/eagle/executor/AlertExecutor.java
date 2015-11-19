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

import org.apache.eagle.alert.common.AlertConstants;
import org.apache.eagle.alert.config.AbstractPolicyDefinition;
import org.apache.eagle.alert.dao.AlertDefinitionDAO;
import org.apache.eagle.alert.dao.AlertStreamSchemaDAOImpl;
import org.apache.eagle.alert.entity.AlertAPIEntity;
import org.apache.eagle.alert.entity.AlertDefinitionAPIEntity;
import org.apache.eagle.common.config.EagleConfigConstants;
import org.apache.eagle.datastream.Collector;
import org.apache.eagle.datastream.JavaStormStreamExecutor2;
import org.apache.eagle.datastream.Tuple2;
import org.apache.eagle.metric.CountingMetric;
import org.apache.eagle.metric.Metric;
import org.apache.eagle.metric.report.EagleSerivceMetricReport;
import com.sun.jersey.client.impl.CopyOnWriteHashMap;
import com.typesafe.config.Config;
import org.apache.eagle.alert.policy.*;
import org.apache.eagle.alert.siddhi.EagleAlertContext;
import org.apache.eagle.alert.siddhi.SiddhiAlertHandler;
import org.apache.eagle.alert.siddhi.StreamMetadataManager;
import org.apache.eagle.dataproc.core.JsonSerDeserUtils;
import org.apache.eagle.dataproc.core.ValuesArray;
import org.apache.commons.lang3.time.DateUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.management.ManagementFactory;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

public class AlertExecutor extends JavaStormStreamExecutor2<String, AlertAPIEntity> implements PolicyLifecycleMethods, SiddhiAlertHandler {
	private static final long serialVersionUID = 1L;

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
	private static String EAGLE_EVENT_COUNT = "eagle.event.count";
	private static String EAGLE_POLICY_EVAL_COUNT = "eagle.policy.eval.count";
	private static String EAGLE_POLICY_EVAL_FAIL_COUNT = "eagle.policy.eval.fail.count";
	private static String EAGLE_ALERT_COUNT = "eagle.alert.count";
	private static String EAGLE_ALERT_FAIL_COUNT = "eagle.alert.fail.count";
	private	 static long MERITE_GRANULARITY = DateUtils.MILLIS_PER_MINUTE;
	private Map<String, Metric> metricMap; // metricMap's key = metricName[#policyId]
	private Map<String, Map<String, String>> dimensionsMap; // cache it for performance
	private Map<String, String> baseDimensions;
	private Thread metricReportThread;
	private EagleSerivceMetricReport metricReport;

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
	
	@Override
	public void prepareConfig(Config config) {
		this.config = config;
	}
	
	public void initMetricReportor() {
		String eagleServiceHost = config.getString(EagleConfigConstants.EAGLE_PROPS + "." + EagleConfigConstants.EAGLE_SERVICE + "." + EagleConfigConstants.HOST);
		int eagleServicePort = config.getInt(EagleConfigConstants.EAGLE_PROPS + "." + EagleConfigConstants.EAGLE_SERVICE + "." + EagleConfigConstants.PORT);

		String username = config.hasPath(EagleConfigConstants.EAGLE_PROPS + "." + EagleConfigConstants.EAGLE_SERVICE + "." + EagleConfigConstants.USERNAME) ?
				          config.getString(EagleConfigConstants.EAGLE_PROPS + "." + EagleConfigConstants.EAGLE_SERVICE + "." + EagleConfigConstants.USERNAME) : null;
		String password = config.hasPath(EagleConfigConstants.EAGLE_PROPS + "." + EagleConfigConstants.EAGLE_SERVICE + "." + EagleConfigConstants.PASSWORD) ?
				          config.getString(EagleConfigConstants.EAGLE_PROPS + "." + EagleConfigConstants.EAGLE_SERVICE + "." + EagleConfigConstants.PASSWORD) : null;
		this.metricReport = new EagleSerivceMetricReport(eagleServiceHost, eagleServicePort, username, password);

		metricMap = new ConcurrentHashMap<String, Metric>();
		baseDimensions = new HashMap<String, String>();
		baseDimensions.put(AlertConstants.ALERT_EXECUTOR_ID, alertExecutorId);
		baseDimensions.put(AlertConstants.PARTITIONSEQ, String.valueOf(partitionSeq));
		baseDimensions.put(AlertConstants.SOURCE, ManagementFactory.getRuntimeMXBean().getName());
		baseDimensions.put(EagleConfigConstants.DATA_SOURCE, config.getString(EagleConfigConstants.EAGLE_PROPS + "." + EagleConfigConstants.DATA_SOURCE));
		baseDimensions.put(EagleConfigConstants.SITE, config.getString(EagleConfigConstants.EAGLE_PROPS + "." + EagleConfigConstants.SITE));

		dimensionsMap = new HashMap<String, Map<String, String>>();
		this.metricReportThread = new Thread() {
			@Override
			public void run() {
				runMetricReporter();
			}
		};
		this.metricReportThread.setDaemon(true);
		metricReportThread.start();
	}

	@Override
	public void init() {
		// initialize StreamMetadataManager before it is used
		StreamMetadataManager.getInstance().init(config, new AlertStreamSchemaDAOImpl(config));
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
		// for efficency, we don't put single policy evaluator 
		policyEvaluators.putAll(tmpPolicyEvaluators);
		DynamicPolicyLoader policyLoader = DynamicPolicyLoader.getInstance();
		
		policyLoader.init(initialAlertDefs, alertDefinitionDao, config);
		policyLoader.addPolicyChangeListener(alertExecutorId + "_" + partitionSeq, this);
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

	public void runMetricReporter() {
		while(true) {
			try {
				long current = System.currentTimeMillis();
				List<Metric> metricList = new ArrayList<Metric>();
				synchronized (this.metricMap) {
					for (Entry<String, Metric> entry : metricMap.entrySet()) {
						String name = entry.getKey();
						Metric metric = entry.getValue();
						long previous = metric.getTimestamp();
						if (current > previous + MERITE_GRANULARITY) {
							metricList.add(metric);
							metricMap.put(name, new CountingMetric(trim(current, MERITE_GRANULARITY), metric.getDemensions(), metric.getMetricName()));
						}
					}
				}
				if (metricList.size() > 0) {
					LOG.info("Going to persist alert metrics, size: " + metricList.size());
					metricReport.emit(metricList);
				}
				try {
					Thread.sleep(MERITE_GRANULARITY / 2);
					} catch (InterruptedException ex) { /* Do nothing */ }
				}
			catch (Throwable t) {
				LOG.error("Got a throwable in metricReporter " , t);
			}
		}
	}

	public void updateCounter(String name, Map<String, String> dimensions, double value) {
		long current = System.currentTimeMillis();
		synchronized (metricMap) {
			if (metricMap.get(name) == null) {
				String metricName = name.split("#")[0];
				metricMap.put(name, new CountingMetric(trim(current, MERITE_GRANULARITY), dimensions, metricName));
			}
			metricMap.get(name).update(value);
		}
	}
	
	public void updateCounter(String name, Map<String, String> dimensions) {
		updateCounter(name, dimensions, 1.0);
	}
	
	public Map<String, String> getDimensions(String policyId) {
		if (dimensionsMap.get(policyId) == null) {
			Map<String, String> newDimensions = new HashMap<String, String>(baseDimensions);
			newDimensions.put(AlertConstants.POLICY_ID, policyId);
			dimensionsMap.put(policyId, newDimensions);
		}
		return dimensionsMap.get(policyId);
	}
	
	public String getMetricKey(String metricName, String policy) {
		return metricName + "#" + policy;
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

        updateCounter(EAGLE_EVENT_COUNT, baseDimensions);
        try{
            synchronized(this.policyEvaluators) {
                for(Entry<String, PolicyEvaluator> entry : policyEvaluators.entrySet()){
                    String policyId = entry.getKey();
                    PolicyEvaluator evaluator = entry.getValue();
                    updateCounter(getMetricKey(EAGLE_POLICY_EVAL_COUNT, policyId), getDimensions(policyId));
                    try {
                        EagleAlertContext siddhiAlertContext = new EagleAlertContext();
                        siddhiAlertContext.alertExecutor = this;
                        siddhiAlertContext.policyId = policyId;
                        siddhiAlertContext.evaluator = evaluator;
                        siddhiAlertContext.outputCollector = outputCollector;
                        evaluator.evaluate(new ValuesArray(siddhiAlertContext, input.get(1), input.get(2)));
                    }
                    catch (Exception ex) {
                        LOG.error("Got an exception, but continue to run " + input.get(2).toString(), ex);
                        updateCounter(getMetricKey(EAGLE_POLICY_EVAL_FAIL_COUNT, policyId), getDimensions(policyId));
                    }
                }
            }
        } catch(Exception ex){
            LOG.error(alertExecutorId + ", partition " + partitionSeq + ", error fetching alerts, but continue to run", ex);
            updateCounter(EAGLE_ALERT_FAIL_COUNT, baseDimensions);
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

	@Override
	public void onAlerts(EagleAlertContext context, List<AlertAPIEntity> alerts) {
		if(alerts != null && !alerts.isEmpty()){
			String policyId = context.policyId;
            LOG.info(String.format("Detected %s alerts for policy %s",alerts.size(),policyId));
			Collector outputCollector = context.outputCollector;
			PolicyEvaluator evaluator = context.evaluator;
			updateCounter(getMetricKey(EAGLE_ALERT_COUNT, policyId), getDimensions(policyId), alerts.size());
			for (AlertAPIEntity entity : alerts) {
				synchronized(this) {
					outputCollector.collect(new Tuple2(policyId, entity));
				}
				if(LOG.isDebugEnabled()) LOG.debug("A new alert is triggered: "+alertExecutorId + ", partition " + partitionSeq + ", Got an alert with output context: " + entity.getAlertContext() + ", for policy " + evaluator);
			}
		}
	}
}
