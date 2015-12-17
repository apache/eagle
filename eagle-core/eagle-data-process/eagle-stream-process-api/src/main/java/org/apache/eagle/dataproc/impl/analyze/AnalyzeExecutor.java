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
package org.apache.eagle.dataproc.impl.analyze;

import org.apache.eagle.alert.dao.PolicyDefinitionDAO;
import org.apache.eagle.alert.policy.PolicyPartitioner;
import org.apache.eagle.dataproc.impl.analyze.entity.AnalyzeDefinitionAPIEntity;
import org.apache.eagle.dataproc.impl.analyze.entity.AnalyzeEntity;
import org.apache.eagle.executor.PolicyProcessExecutor;

/**
 * @since Dec 16, 2015
 *
 */
public class AnalyzeExecutor extends PolicyProcessExecutor<AnalyzeDefinitionAPIEntity, AnalyzeEntity> 

//extends JavaStormStreamExecutor2<String, AnalyzeDefinitionAPIEntity> 
//		implements PolicyLifecycleMethods, SiddhiAlertHandler, EagleExecutor<AnalyzeDefinitionAPIEntity> 
{

	public AnalyzeExecutor(String alertExecutorId, PolicyPartitioner partitioner, int numPartitions, int partitionSeq,
			PolicyDefinitionDAO<AnalyzeDefinitionAPIEntity> alertDefinitionDao, String[] sourceStreams) {
		super(alertExecutorId, partitioner, numPartitions, partitionSeq, alertDefinitionDao, sourceStreams);
	}

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	
//	private static final long serialVersionUID = 1L;
//
//	private final int partitionSeq;
//	private final String givenCepQl;
//
//	public AnalyzeExecutor(int partitionSeq, String givenCepQl) {
//		this.partitionSeq = partitionSeq;
//		this.givenCepQl = givenCepQl;
//	}
//
//	public String getExecutorId() {
//		return "";
//	}
//	
//	public int getPartitionSeq() {
//		return partitionSeq;
//	}
//
//	@Override
//    public void flatMap(List<Object> input, Collector<Tuple2<String, AnalyzeDefinitionAPIEntity>> outputCollector) {
//		
//	}
//
//	@Override
//	public void onAlerts(EagleAlertContext context, List<AlertAPIEntity> alerts) {
//		// TODO Auto-generated method stub
//		
//	}
//
//	@Override
//	public void onPolicyCreated(Map<String, AlertDefinitionAPIEntity> added) {
//		// TODO Auto-generated method stub
//		
//	}
//
//	@Override
//	public void onPolicyChanged(Map<String, AlertDefinitionAPIEntity> changed) {
//		// TODO Auto-generated method stub
//		
//	}
//
//	@Override
//	public void onPolicyDeleted(Map<String, AlertDefinitionAPIEntity> deleted) {
//		// TODO Auto-generated method stub
//		
//	}
//
//	@Override
//	public void prepareConfig(Config config) {
//		// TODO Auto-generated method stub
//		
//	}
//
//	@Override
//	public void init() {
//		// TODO Auto-generated method stub
//		
//	}

}
