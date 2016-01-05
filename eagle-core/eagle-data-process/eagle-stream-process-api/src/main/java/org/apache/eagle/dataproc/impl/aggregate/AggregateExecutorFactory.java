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

import com.typesafe.config.Config;
import com.typesafe.config.ConfigValue;
import org.apache.eagle.dataproc.impl.aggregate.entity.AggregateDefinitionAPIEntity;
import org.apache.eagle.policy.DefaultPolicyPartitioner;
import org.apache.eagle.policy.PolicyPartitioner;
import org.apache.eagle.policy.common.Constants;
import org.apache.eagle.policy.dao.PolicyDefinitionDAO;
import org.apache.eagle.policy.dao.PolicyDefinitionEntityDAOImpl;
import org.apache.eagle.service.client.EagleServiceConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * @since Dec 16, 2015
 *
 */
public class AggregateExecutorFactory {

	private static final Logger LOG = LoggerFactory.getLogger(AggregateExecutorFactory.class);
	
	private AggregateExecutorFactory() {}
	public static final AggregateExecutorFactory Instance = new AggregateExecutorFactory();


	public AggregateExecutor[] createExecutors(Config config, List<String> streamNames, String executorId) throws Exception {
//		// Read site and dataSource from configuration.
//		String dataSource = config.getString(EagleConfigConstants.EAGLE_PROPS + "." + EagleConfigConstants.DATA_SOURCE);
//		LOG.info("Loading alerting definitions for dataSource: " + dataSource);
//		List<String> streamNames = findStreamNames(config, executorId, dataSource);
//		if (streamNames.isEmpty()) {
//			throw new IllegalStateException("upstream names should not be empty for analyze executor " + executorId);
//		}

		StringBuilder partitionerCls = new StringBuilder(DefaultPolicyPartitioner.class.getCanonicalName());
        int numPartitions = loadExecutorConfig(config, executorId, partitionerCls);
        
		PolicyDefinitionDAO<AggregateDefinitionAPIEntity> policyDefDao = new PolicyDefinitionEntityDAOImpl<AggregateDefinitionAPIEntity>(
				new EagleServiceConnector(config), Constants.AGGREGATE_DEFINITION_SERVICE_ENDPOINT_NAME);
		
		
		return newAggregateExecutors(policyDefDao, streamNames, executorId, numPartitions, partitionerCls.toString());
	}
	
	@SuppressWarnings("unchecked")
	private int loadExecutorConfig(Config config, String executorId, StringBuilder partitionerCls) {
		int numPartitions = 0;
		String aggregateExecutorConfigsKey = "aggregateExecutorConfigs";
        if(config.hasPath(aggregateExecutorConfigsKey)) {
            Map<String, ConfigValue> analyzeExecutorConfigs = config.getObject(aggregateExecutorConfigsKey);
            if(analyzeExecutorConfigs !=null && analyzeExecutorConfigs.containsKey(executorId)) {
                Map<String, Object> alertExecutorConfig = (Map<String, Object>) analyzeExecutorConfigs.get(executorId).unwrapped();
                int parts = 0;
                if(alertExecutorConfig.containsKey("parallelism")) parts = (int) (alertExecutorConfig.get("parallelism"));
                numPartitions = parts == 0 ? 1 : parts;
                if(alertExecutorConfig.containsKey("partitioner")) {
                	partitionerCls.setLength(0);
                	partitionerCls.append((String) alertExecutorConfig.get("partitioner"));
                }
            }
        }
        return numPartitions;
	}


//	private List<String> findStreamNames(Config config, String executorId, String dataSource) throws Exception {
//		// Get map from alertExecutorId to alert stream
//		// (dataSource) => Map[alertExecutorId:String,streamName:List[String]]
//		List<String> streamNames = new ArrayList<String>();
//		// FIXME : here we reuse the executor definition. But the name alert is not ambiguous now.
//		AlertExecutorDAOImpl alertExecutorDAO = new AlertExecutorDAOImpl(new EagleServiceConnector(config));
//		List<AlertExecutorEntity> alertExecutorEntities = alertExecutorDAO.findAlertExecutor(dataSource,
//				executorId);
//		for (AlertExecutorEntity entity : alertExecutorEntities) {
//			streamNames.add(entity.getTags().get(Constants.STREAM_NAME));
//		}
//		return streamNames;
//	}
	
	private AggregateExecutor[] newAggregateExecutors(PolicyDefinitionDAO<AggregateDefinitionAPIEntity> alertDefDAO,
			List<String> sourceStreams, String executorID, int numPartitions, String partitionerCls)
					throws Exception {
		LOG.info("Creating alert executors with executorID: " + executorID + ", numPartitions: "
				+ numPartitions + ", Partition class is: " + partitionerCls);

		PolicyPartitioner partitioner = (PolicyPartitioner) Class.forName(partitionerCls).newInstance();
		AggregateExecutor[] alertExecutors = new AggregateExecutor[numPartitions];
		String[] _sourceStreams = sourceStreams.toArray(new String[0]);

		for (int i = 0; i < numPartitions; i++) {
			alertExecutors[i] = new AggregateExecutor(executorID, partitioner, numPartitions, i, alertDefDAO,
					_sourceStreams);
		}
		return alertExecutors;
	}

}
