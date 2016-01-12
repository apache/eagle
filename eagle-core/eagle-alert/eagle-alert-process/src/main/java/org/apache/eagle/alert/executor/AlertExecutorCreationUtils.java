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
package org.apache.eagle.alert.executor;

import java.util.List;
import java.util.Map;

import org.apache.eagle.policy.dao.PolicyDefinitionDAO;
import org.apache.eagle.alert.entity.AlertDefinitionAPIEntity;
import org.apache.eagle.policy.DefaultPolicyPartitioner;
import org.apache.eagle.policy.PolicyPartitioner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigValue;

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
	public static AlertExecutor[] createAlertExecutors(Config config, PolicyDefinitionDAO<AlertDefinitionAPIEntity> alertDefDAO,
			List<String> streamNames, String alertExecutorId) throws Exception{
		// Read `alertExecutorConfigs` from configuration and get config for this alertExecutorId
        int numPartitions =1;
        String partitionerCls = DefaultPolicyPartitioner.class.getCanonicalName();
        String alertExecutorConfigsKey = "alertExecutorConfigs";
        if(config.hasPath(alertExecutorConfigsKey)) {
            Map<String, ConfigValue> alertExecutorConfigs = config.getObject(alertExecutorConfigsKey);
            if(alertExecutorConfigs !=null && alertExecutorConfigs.containsKey(alertExecutorId)) {
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
	public static AlertExecutor[] createAlertExecutors(PolicyDefinitionDAO alertDefDAO, List<String> sourceStreams,
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
