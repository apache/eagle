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

import org.apache.eagle.policy.ResultRender;
import org.apache.eagle.policy.dao.PolicyDefinitionDAO;
import org.apache.eagle.policy.PolicyPartitioner;
import org.apache.eagle.dataproc.impl.aggregate.entity.AggregateDefinitionAPIEntity;
import org.apache.eagle.dataproc.impl.aggregate.entity.AggregateEntity;
import org.apache.eagle.policy.executor.PolicyProcessExecutor;

/**
 * @since Dec 16, 2015
 *
 */
public class AggregateExecutor extends PolicyProcessExecutor<AggregateDefinitionAPIEntity, AggregateEntity> {

	private static final long serialVersionUID = 1L;

	private ResultRender<AggregateDefinitionAPIEntity, AggregateEntity> render = new AggregateResultRender();

	public AggregateExecutor(String executorId, PolicyPartitioner partitioner, int numPartitions, int partitionSeq,
			PolicyDefinitionDAO<AggregateDefinitionAPIEntity> alertDefinitionDao, String[] sourceStreams) {
		super(executorId, partitioner, numPartitions, partitionSeq, alertDefinitionDao, sourceStreams,
				AggregateDefinitionAPIEntity.class);
	}

	@Override
	public ResultRender<AggregateDefinitionAPIEntity, AggregateEntity> getResultRender() {
		return render;
	}
}
