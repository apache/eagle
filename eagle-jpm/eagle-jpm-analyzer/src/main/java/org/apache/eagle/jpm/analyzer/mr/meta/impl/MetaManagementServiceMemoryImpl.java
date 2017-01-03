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

package org.apache.eagle.jpm.analyzer.mr.meta.impl;

import com.typesafe.config.Config;
import org.apache.eagle.jpm.analyzer.mr.meta.MetaManagementService;
import org.apache.eagle.jpm.analyzer.mr.meta.model.JobMetaEntity;
import org.apache.eagle.jpm.analyzer.mr.meta.model.PublisherEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MetaManagementServiceMemoryImpl implements MetaManagementService {
    private static final Logger LOG = LoggerFactory.getLogger(MetaManagementServiceMemoryImpl.class);

    private Map<String, JobMetaEntity> jobMetaEntities = new HashMap<>();
    private Map<String, List<PublisherEntity>> publisherEntities = new HashMap<>();
    private Config config;

    public MetaManagementServiceMemoryImpl(Config config) {
        this.config = config;
    }

    @Override
    public boolean addJobMeta(JobMetaEntity jobMetaEntity) {
        if (jobMetaEntities.containsKey(jobMetaEntity.getJobDefId())) {
            LOG.warn("contains job {} already", jobMetaEntity.getJobDefId());
            return false;
        }

        jobMetaEntities.put(jobMetaEntity.getJobDefId(), jobMetaEntity);
        return true;
    }

    @Override
    public boolean updateJobMeta(String jobDefId, JobMetaEntity jobMetaEntity) {
        if (!jobMetaEntities.containsKey(jobMetaEntity.getJobDefId())) {
            LOG.warn("does not contain job {}", jobDefId);
            return false;
        }

        jobMetaEntities.put(jobDefId, jobMetaEntity);
        return true;
    }

    @Override
    public JobMetaEntity getJobMeta(String jobDefId) {
        if (!jobMetaEntities.containsKey(jobDefId)) {
            LOG.warn("does not contain job {}", jobDefId);
            return null;
        }

        return jobMetaEntities.get(jobDefId);
    }

    @Override
    public boolean deleteJobMeta(String jobDefId) {
        if (!jobMetaEntities.containsKey(jobDefId)) {
            LOG.warn("does not contain job {}", jobDefId);
            return false;
        }

        jobMetaEntities.remove(jobDefId);
        return true;
    }

    @Override
    public boolean addPublisherMeta(PublisherEntity publisherEntity) {
        if (publisherEntities.containsKey(publisherEntity.getUserId())) {
            for (PublisherEntity entity : publisherEntities.get(publisherEntity.getUserId())) {
                if (entity.equals(publisherEntity)) {
                    LOG.warn("contains user {}, mailAddress {} already", entity.getUserId(), entity.getMailAddress());
                    return false;
                }
            }
        }

        if (!publisherEntities.containsKey(publisherEntity.getUserId())) {
            publisherEntities.put(publisherEntity.getUserId(), new ArrayList<>());
        }

        publisherEntities.get(publisherEntity.getUserId()).add(publisherEntity);
        return true;
    }

    @Override
    public boolean deletePublisherMeta(String userId) {
        if (!publisherEntities.containsKey(userId)) {
            LOG.warn("does not contain user {}", userId);
            return false;
        }

        publisherEntities.remove(userId);
        return true;
    }

    @Override
    public List<PublisherEntity> getPublisherMeta(String userId) {
        if (!publisherEntities.containsKey(userId)) {
            LOG.warn("does not contain user {}", userId);
            return null;
        }

        return publisherEntities.get(userId);
    }
}
