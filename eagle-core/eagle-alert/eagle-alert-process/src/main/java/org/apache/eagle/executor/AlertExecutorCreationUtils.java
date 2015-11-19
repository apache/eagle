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
import org.apache.eagle.alert.dao.AlertDefinitionDAO;
import org.apache.eagle.alert.dao.AlertDefinitionDAOImpl;
import org.apache.eagle.alert.dao.AlertExecutorDAOImpl;
import org.apache.eagle.alert.entity.AlertExecutorEntity;
import org.apache.eagle.alert.policy.DefaultPolicyPartitioner;
import org.apache.eagle.alert.policy.PolicyPartitioner;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigValue;
import org.apache.eagle.common.config.EagleConfigConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Create alert executors and provide callback for programmer to link alert executor to immediate parent executors
 *
 * <br/><br/>
 * Explanations for programId, alertExecutorId and policy<br/><br/>
 * - programId - distributed or single-process program for example one storm topology<br/>
 * - alertExecutorId - one process/thread which executes multiple policies<br/>
 * - policy - some rules to be evaluated<br/>
 *
 * <br/>
 *
 * Normally the mapping is like following:
 * <pre>
 * programId (1:N) alertExecutorId
 * alertExecutorId (1:N) policy
 * </pre>
 */
public class AlertExecutorCreationUtils {
	private final static Logger LOG = LoggerFactory.getLogger(AlertExecutorCreationUtils.class);

    public static AlertExecutor[] createAlertExecutors(Config config, String alertExecutorId) throws Exception{
        // Read site and dataSource from configuration.
        String dataSource = config.getString(EagleConfigConstants.EAGLE_PROPS + "." + EagleConfigConstants.DATA_SOURCE);
        LOG.info("Loading alerting definitions for dataSource: " + dataSource);

        // Get map from alertExecutorId to alert stream
        // (dataSource) => Map[alertExecutorId:String,streamName:List[String]]
        List<String> streamNames = new ArrayList<String>();
        AlertExecutorDAOImpl alertExecutorDAO = new AlertExecutorDAOImpl(config);
        List<AlertExecutorEntity> alertExecutorEntities = alertExecutorDAO.findAlertExecutor(dataSource, alertExecutorId);
        for(AlertExecutorEntity entity : alertExecutorEntities){
            streamNames.add(entity.getTags().get(AlertConstants.STREAM_NAME));
        }

        if(streamNames.isEmpty()){
            throw new IllegalStateException("upstream names should not be empty for alert " + alertExecutorId);
        }
        return createAlertExecutors(config, new AlertDefinitionDAOImpl(config),
                streamNames, alertExecutorId);
    }

    /**
     * Build DAG Tasks based on persisted alert definition and schemas from eagle store.
     *
     * <h3>Require configuration:</h3>
     *
     * <ul>
     * <li>eagleProps.site: program site id.</li>
     * <li>eagleProps.dataSource: program data source.</li>
     * <li>alertExecutorConfigs: only configured executor will be built into execution tasks.</li>
     * </ul>
     *
     * <h3>Steps:</h3>
     *
     * <ol>
     * <li>(upstreamTasks) => Map[streamName:String,upstreamTask:Task]</li>
     * <li>(dataSource) => Map[alertExecutorId:String,streamName:List[String]]</li>
     * <li>(site,dataSource) => Map[alertExecutorId,Map[policyId,alertDefinition]]</li>
     * <li>(config["alertExecutorConfigs"]) => AlertExecutor(alertExecutorID, partitioner, numPartitions, partitionSeq, alertDefs, alertDefDAO, sourceStreams)[]</li>
     * </ol>
     */
	public static AlertExecutor[] createAlertExecutors(Config config, AlertDefinitionDAO alertDefDAO,
			List<String> streamNames, String alertExecutorId) throws Exception{
		// Read `alertExecutorConfigs` from configuration and get config for this alertExecutorId
        int numPartitions =1;
        String partitionerCls = DefaultPolicyPartitioner.class.getCanonicalName();
        String alertExecutorConfigsKey = "alertExecutorConfigs";
        if(config.hasPath(alertExecutorConfigsKey)) {
            Map<String, ConfigValue> alertExecutorConfigs = config.getObject(alertExecutorConfigsKey);
            if(alertExecutorConfigs !=null && alertExecutorConfigs.containsKey(alertExecutorConfigs)) {
                Map<String, Object> alertExecutorConfig = (Map<String, Object>) alertExecutorConfigs.get(alertExecutorId).unwrapped();
                int parts = 0;
                if(alertExecutorConfig.containsKey("parallelism")) parts = (int) (alertExecutorConfig.get("parallelism"));
                numPartitions = parts == 0 ? 1 : parts;
                if(alertExecutorConfig.containsKey("partitioner")) partitionerCls = (String) alertExecutorConfig.get("partitioner");
            }
        }

        return createAlertExecutors(alertDefDAO, streamNames, alertExecutorId, numPartitions, partitionerCls);
	}

    /**
     * Build alert executors and assign alert definitions between these executors by partitioner (alertExecutorConfigs["${alertExecutorId}"]["partitioner"])
     */
	public static AlertExecutor[] createAlertExecutors(AlertDefinitionDAO alertDefDAO, List<String> sourceStreams,
                                                          String alertExecutorID, int numPartitions, String partitionerCls) throws Exception{
		LOG.info("Creating alert executors with alertExecutorID: " + alertExecutorID + ", numPartitions: " + numPartitions + ", Partition class is: "+ partitionerCls);

		PolicyPartitioner partitioner = (PolicyPartitioner)Class.forName(partitionerCls).newInstance();
		AlertExecutor[] alertExecutors = new AlertExecutor[numPartitions];
        String[] _sourceStreams = sourceStreams.toArray(new String[0]);

		for(int i = 0; i < numPartitions; i++){
			alertExecutors[i] = new AlertExecutor(alertExecutorID, partitioner, numPartitions, i, alertDefDAO,_sourceStreams);
		}	
		return alertExecutors;
	}
}
