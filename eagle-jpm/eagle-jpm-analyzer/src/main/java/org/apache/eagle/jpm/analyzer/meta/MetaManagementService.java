/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.eagle.jpm.analyzer.meta;

import org.apache.eagle.jpm.analyzer.meta.model.JobMetaEntity;
import org.apache.eagle.jpm.analyzer.meta.model.PublisherEntity;

import java.util.List;

public interface MetaManagementService {
    boolean addJobMeta(JobMetaEntity jobMetaEntity);

    boolean updateJobMeta(String jobDefId, JobMetaEntity jobMetaEntity);

    List<JobMetaEntity> getJobMeta(String jobDefId);

    boolean deleteJobMeta(String jobDefId);

    boolean addPublisherMeta(PublisherEntity publisherEntity);

    boolean deletePublisherMeta(String userId);

    List<PublisherEntity> getPublisherMeta(String userId);
}
