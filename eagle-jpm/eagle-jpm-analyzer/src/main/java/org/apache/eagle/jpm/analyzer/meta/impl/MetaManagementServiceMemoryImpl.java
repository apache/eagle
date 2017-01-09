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

package org.apache.eagle.jpm.analyzer.meta.impl;

import com.google.inject.Inject;
import com.typesafe.config.Config;
import org.apache.eagle.jpm.analyzer.meta.MetaManagementService;
import org.apache.eagle.jpm.analyzer.meta.model.JobMetaEntity;
import org.apache.eagle.jpm.analyzer.meta.model.PublisherEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.*;

public class MetaManagementServiceMemoryImpl implements MetaManagementService, Serializable {
    private static final Logger LOG = LoggerFactory.getLogger(MetaManagementServiceMemoryImpl.class);

    private final Map<String, JobMetaEntity> jobMetaEntities = new HashMap<>();
    private final Map<String, List<PublisherEntity>> publisherEntities = new HashMap<>();

    @Inject
    Config config;

    @Override
    public boolean addJobMeta(JobMetaEntity jobMetaEntity) {
        if (jobMetaEntities.containsKey(jobMetaEntity.getJobDefId())) {
            LOG.warn("contains job {} already, add job meta failed", jobMetaEntity.getJobDefId());
            return false;
        }

        jobMetaEntities.put(jobMetaEntity.getJobDefId(), jobMetaEntity);
        LOG.info("Successfully add job {} meta", jobMetaEntity.getJobDefId());
        return true;
    }

    @Override
    public boolean updateJobMeta(String jobDefId, JobMetaEntity jobMetaEntity) {
        if (!jobMetaEntities.containsKey(jobMetaEntity.getJobDefId())) {
            LOG.warn("does not contain job {}, update job meta failed", jobDefId);
            return false;
        }

        jobMetaEntities.put(jobDefId, jobMetaEntity);
        LOG.info("Successfully update job {} meta", jobDefId);
        return true;
    }

    @Override
    public List<JobMetaEntity> getJobMeta(String jobDefId) {
        if (!jobMetaEntities.containsKey(jobDefId)) {
            LOG.warn("does not contain job {}, get job meta failed", jobDefId);
            return new ArrayList<>();
        }

        return Arrays.asList(jobMetaEntities.get(jobDefId));
    }

    @Override
    public boolean deleteJobMeta(String jobDefId) {
        if (!jobMetaEntities.containsKey(jobDefId)) {
            LOG.warn("does not contain job {}, delete job meta failed", jobDefId);
            return false;
        }

        jobMetaEntities.remove(jobDefId);
        LOG.info("Successfully delete job {} meta", jobDefId);
        return true;
    }

    @Override
    public boolean addPublisherMeta(PublisherEntity publisherEntity) {
        if (publisherEntities.containsKey(publisherEntity.getUserId())) {
            for (PublisherEntity entity : publisherEntities.get(publisherEntity.getUserId())) {
                if (entity.equals(publisherEntity)) {
                    LOG.warn("contains user {}, mailAddress {} already, add publisher failed", entity.getUserId(), entity.getMailAddress());
                    return false;
                }
            }
        }

        if (!publisherEntities.containsKey(publisherEntity.getUserId())) {
            publisherEntities.put(publisherEntity.getUserId(), new ArrayList<>());
        }

        publisherEntities.get(publisherEntity.getUserId()).add(publisherEntity);
        LOG.info("Successfully add publisher user {}, mailAddress {}", publisherEntity.getUserId(), publisherEntity.getMailAddress());
        return true;
    }

    @Override
    public boolean deletePublisherMeta(String userId) {
        if (!publisherEntities.containsKey(userId)) {
            LOG.warn("does not contain user {}, failed to delete publisher", userId);
            return false;
        }

        publisherEntities.remove(userId);
        LOG.info("Successfully delete publisher user " + userId);
        return true;
    }

    @Override
    public List<PublisherEntity> getPublisherMeta(String userId) {
        if (!publisherEntities.containsKey(userId)) {
            LOG.warn("does not contain user {}, failed to get publisher", userId);
            return new ArrayList<>();
        }

        return publisherEntities.get(userId);
    }
}
