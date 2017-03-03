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

import com.typesafe.config.Config;
import org.apache.eagle.jpm.analyzer.meta.MetaManagementService;
import org.apache.eagle.jpm.analyzer.meta.model.UserEmailEntity;
import org.apache.eagle.jpm.analyzer.meta.model.JobMetaEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.io.Serializable;
import java.util.*;

public class MetaManagementServiceMemoryImpl implements MetaManagementService, Serializable {
    private static final Logger LOG = LoggerFactory.getLogger(MetaManagementServiceMemoryImpl.class);

    private final Map<String, Map<String, JobMetaEntity>> jobMetaEntities = new HashMap<>();
    private final Map<String, Map<String, UserEmailEntity>> publisherEntities = new HashMap<>();

    @Inject
    Config config;

    @Override
    public boolean addJobMeta(JobMetaEntity jobMetaEntity) {
        if (!jobMetaEntities.containsKey(jobMetaEntity.getSiteId())) {
            jobMetaEntities.put(jobMetaEntity.getSiteId(), new HashMap<>());
        }

        jobMetaEntities.get(jobMetaEntity.getSiteId()).put(jobMetaEntity.getJobDefId(), jobMetaEntity);
        LOG.info("Successfully add job {} meta", jobMetaEntity.getJobDefId());
        return true;
    }

    @Override
    public boolean updateJobMeta(JobMetaEntity jobMetaEntity) {
        if (!jobMetaEntities.containsKey(jobMetaEntity.getSiteId())) {
            LOG.warn("does not contain siteId {}, update job meta failed", jobMetaEntity.getSiteId());
            return false;
        }

        jobMetaEntities.get(jobMetaEntity.getSiteId()).put(jobMetaEntity.getJobDefId(), jobMetaEntity);
        LOG.info("Successfully update job {} meta", jobMetaEntity.getJobDefId());
        return true;
    }

    @Override
    public List<JobMetaEntity> getJobMeta(String siteId, String jobDefId) {
        if (!jobMetaEntities.containsKey(siteId)) {
            LOG.warn("does not contain site {}, get job meta failed", siteId);
            return new ArrayList<>();
        }

        return Arrays.asList(jobMetaEntities.get(siteId).get(jobDefId));
    }

    @Override
    public boolean deleteJobMeta(String siteId, String jobDefId) {
        if (!jobMetaEntities.containsKey(siteId)) {
            LOG.warn("does not contain siteId {}, delete job meta failed", siteId);
            return false;
        }

        jobMetaEntities.get(siteId).remove(jobDefId);
        LOG.info("Successfully delete job {} meta", jobDefId);
        return true;
    }

    @Override
    public boolean addUserEmailMeta(UserEmailEntity userEmailEntity) {
        if (publisherEntities.containsKey(userEmailEntity.getSiteId())) {
            for (UserEmailEntity entity : publisherEntities.get(userEmailEntity.getSiteId()).values()) {
                if (entity.equals(userEmailEntity)) {
                    LOG.warn("contains user {}, mailAddress {} already, add publisher failed", entity.getUserId(), entity.getMailAddress());
                    return false;
                }
            }
        }

        if (!publisherEntities.containsKey(userEmailEntity.getSiteId())) {
            publisherEntities.put(userEmailEntity.getSiteId(), new HashMap<>());
        }

        publisherEntities.get(userEmailEntity.getSiteId()).put(userEmailEntity.getUserId(), userEmailEntity);
        LOG.info("Successfully add publisher user {}, mailAddress {}", userEmailEntity.getUserId(), userEmailEntity.getMailAddress());
        return true;
    }

    @Override
    public boolean updateUserEmailMeta(UserEmailEntity userEmailEntity) {
        if (!publisherEntities.containsKey(userEmailEntity.getSiteId())) {
            LOG.warn("does not contain siteId {}, update user email meta failed", userEmailEntity.getSiteId());
            return false;
        }

        publisherEntities.get(userEmailEntity.getSiteId()).put(userEmailEntity.getUserId(), userEmailEntity);
        LOG.info("Successfully update user {} meta", userEmailEntity.getUserId());
        return true;
    }

    @Override
    public boolean deleteUserEmailMeta(String siteId, String userId) {
        if (!publisherEntities.containsKey(userId)) {
            LOG.warn("does not contain user {}, failed to delete publisher", userId);
            return false;
        }

        publisherEntities.remove(userId);
        LOG.info("Successfully delete publisher user " + userId);
        return true;
    }

    @Override
    public List<UserEmailEntity> getUserEmailMeta(String siteId, String userId) {
        if (!publisherEntities.containsKey(siteId)) {
            LOG.warn("does not contain siteId {}, failed to get publisher", siteId);
            return new ArrayList<>();
        }

        return Arrays.asList(publisherEntities.get(siteId).get(userId));
    }
}
