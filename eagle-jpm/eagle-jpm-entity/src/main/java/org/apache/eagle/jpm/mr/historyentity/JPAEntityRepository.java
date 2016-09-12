/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/

package org.apache.eagle.jpm.mr.historyentity;

import org.apache.eagle.jpm.util.jobcounter.JobCounters;
import org.apache.eagle.jpm.util.jobcounter.JobCountersSerDeser;
import org.apache.eagle.log.entity.repo.EntityRepository;

public class JPAEntityRepository extends EntityRepository {

    public JPAEntityRepository() {
        serDeserMap.put(JobCounters.class, new JobCountersSerDeser());
        serDeserMap.put(JobConfig.class, new JobConfigSerDeser());
        entitySet.add(JobConfigurationAPIEntity.class);
        entitySet.add(JobEventAPIEntity.class);
        entitySet.add(JobExecutionAPIEntity.class);

        entitySet.add(TaskAttemptExecutionAPIEntity.class);
        entitySet.add(TaskExecutionAPIEntity.class);
        entitySet.add(TaskFailureCountAPIEntity.class);
        entitySet.add(TaskAttemptCounterAPIEntity.class);
        entitySet.add(JobProcessTimeStampEntity.class);
        entitySet.add(JobCountEntity.class);
    }
}
