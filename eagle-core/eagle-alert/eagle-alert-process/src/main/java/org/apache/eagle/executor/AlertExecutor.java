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

import org.apache.eagle.alert.dao.PolicyDefinitionDAO;
import org.apache.eagle.alert.entity.AlertAPIEntity;
import org.apache.eagle.alert.entity.AlertDefinitionAPIEntity;
import org.apache.eagle.alert.policy.PolicyPartitioner;

public class AlertExecutor extends PolicyProcessExecutor<AlertDefinitionAPIEntity, AlertAPIEntity> {

	public AlertExecutor(String alertExecutorId, PolicyPartitioner partitioner, int numPartitions, int partitionSeq,
			PolicyDefinitionDAO<AlertDefinitionAPIEntity> alertDefinitionDao, String[] sourceStreams) {
		super(alertExecutorId, partitioner, numPartitions, partitionSeq, alertDefinitionDao, sourceStreams,
				AlertDefinitionAPIEntity.class);
	}
}
