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
import org.apache.commons.lang.StringUtils;
import org.apache.eagle.jpm.analyzer.meta.MetaManagementService;
import org.apache.eagle.jpm.analyzer.meta.impl.orm.JobMetaEntityToRelation;
import org.apache.eagle.jpm.analyzer.meta.impl.orm.RelationToJobMetaEntity;
import org.apache.eagle.jpm.analyzer.meta.impl.orm.RelationToUserEmailEntity;
import org.apache.eagle.jpm.analyzer.meta.impl.orm.UserEmailEntityToRelation;
import org.apache.eagle.jpm.analyzer.meta.model.JobMetaEntity;
import org.apache.eagle.jpm.analyzer.meta.model.UserEmailEntity;
import org.apache.eagle.metadata.store.jdbc.JDBCMetadataQueryService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.io.Serializable;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class MetaManagementServiceJDBCImpl implements MetaManagementService, Serializable {
    private static final Logger LOG = LoggerFactory.getLogger(MetaManagementServiceJDBCImpl.class);

    private static final String addJobMetaSql = "INSERT INTO analysis_jobs(uuid, configuration, evaluators, createdtime, modifiedtime, siteId, jobDefId) VALUES (?, ?, ?, ?, ?, ?, ?)";
    private static final String addUserEmailSql = "INSERT INTO analysis_email(uuid, mailAddress, createdtime, modifiedtime, siteId, userId) VALUES (?, ?, ?, ?, ?, ?)";

    private static final String getJobMetaSql = "SELECT * FROM analysis_jobs where siteId = ? and jobDefId = ?";
    private static final String getUserEmailSql = "SELECT * FROM analysis_email where siteId = ? and userId = ?";

    private static final String deleteJobMetaSql = "DELETE FROM analysis_jobs where siteId = ? and jobDefId = ?";
    private static final String deleteUserEmailSql = "DELETE FROM analysis_email where siteId = ? and userId = ?";

    @Inject
    Config config;

    @Inject
    JDBCMetadataQueryService queryService;

    @Override
    public boolean addJobMeta(JobMetaEntity jobMetaEntity) {
        if (getJobMeta(jobMetaEntity.getSiteId(), jobMetaEntity.getJobDefId()) != null) {
            throw new IllegalArgumentException("Duplicated job meta: " + jobMetaEntity.getSiteId() + ": " + jobMetaEntity.getJobDefId());
        }

        List<JobMetaEntity> entities = new ArrayList<>(1);
        entities.add(jobMetaEntity);
        try {
            queryService.insert(addJobMetaSql, entities, new JobMetaEntityToRelation());
        } catch (SQLException e) {
            LOG.error("Error to insert JobMetaEntity: {}", jobMetaEntity, e);
            return false;
        }
        return true;
    }

    @Override
    public boolean updateJobMeta(JobMetaEntity entity) {
        String updateSql = "update analysis_jobs set ";
        if (entity.getUuid() != null && !entity.getUuid().isEmpty()) {
            updateSql += "uuid = ?, ";
        }
        if (entity.getConfiguration() != null) {
            updateSql += "configuration = ?, ";
        }
        if (entity.getEvaluators() != null) {
            updateSql += "evaluators = ?, ";
        }
        if (entity.getCreatedTime() > 0) {
            updateSql += "createdtime = ?, ";
        }
        if (entity.getModifiedTime() > 0) {
            updateSql += "modifiedtime = ?, ";
        }
        updateSql = updateSql.substring(0, updateSql.length() - 2);
        if (StringUtils.isNotBlank(entity.getSiteId())) {
            updateSql += " where siteId = ?";
        }
        if (StringUtils.isNotBlank(entity.getJobDefId())) {
            updateSql += " and jobDefId = ?";
        }

        try {
            if (queryService.update(updateSql, entity, new JobMetaEntityToRelation()) == 0) {
                LOG.warn("failed to execute {}", updateSql);
            }
        } catch (SQLException e) {
            LOG.warn("failed to execute {}, {}", updateSql, e);
            return false;
        }
        return true;
    }

    @Override
    public List<JobMetaEntity> getJobMeta(String siteId, String jobDefId) {
        JobMetaEntity jobMetaEntity = new JobMetaEntity();
        jobMetaEntity.setSiteId(siteId);
        jobMetaEntity.setJobDefId(jobDefId);

        List<JobMetaEntity> results;
        try {
            results = queryService.queryWithCond(getJobMetaSql, jobMetaEntity, new JobMetaEntityToRelation(), new RelationToJobMetaEntity());
        } catch (SQLException e) {
            LOG.error("Error to getJobMeta : {}", e);
            return null;
        }
        if (results.isEmpty()) {
            return null;
        }

        return results;
    }

    @Override
    public boolean deleteJobMeta(String siteId, String jobDefId) {
        JobMetaEntity entity = new JobMetaEntity();
        entity.setSiteId(siteId);
        entity.setJobDefId(jobDefId);
        try {
            queryService.update(deleteJobMetaSql, entity, new JobMetaEntityToRelation());
        } catch (SQLException e) {
            LOG.error("Error to delete JobMetaEntity: {}", entity, e);
            return false;
        }

        return true;
    }

    @Override
    public boolean addUserEmailMeta(UserEmailEntity userEmailEntity) {
        if (getUserEmailMeta(userEmailEntity.getSiteId(), userEmailEntity.getUserId()) != null) {
            throw new IllegalArgumentException("Duplicated user meta: " + userEmailEntity.getSiteId() + ": " + userEmailEntity.getUserId());
        }

        List<UserEmailEntity> entities = new ArrayList<>(1);
        entities.add(userEmailEntity);
        try {
            queryService.insert(addUserEmailSql, entities, new UserEmailEntityToRelation());
        } catch (SQLException e) {
            LOG.error("Error to insert UserEmailEntity: {}", userEmailEntity, e);
            return false;
        }
        return true;
    }

    @Override
    public  boolean updateUserEmailMeta(UserEmailEntity entity) {
        String updateSql = "update analysis_email set ";
        if (entity.getUuid() != null && !entity.getUuid().isEmpty()) {
            updateSql += "uuid = ?, ";
        }
        if (entity.getMailAddress() != null && !entity.getMailAddress().isEmpty()) {
            updateSql += "mailAddress = ?, ";
        }
        if (entity.getCreatedTime() > 0) {
            updateSql += "createdtime = ?, ";
        }
        if (entity.getModifiedTime() > 0) {
            updateSql += "modifiedtime = ?, ";
        }
        updateSql = updateSql.substring(0, updateSql.length() - 2);
        if (StringUtils.isNotBlank(entity.getSiteId())) {
            updateSql += " where siteId = ?";
        }
        if (StringUtils.isNotBlank(entity.getUserId())) {
            updateSql += " and userId = ?";
        }

        try {
            if (queryService.update(updateSql, entity, new UserEmailEntityToRelation()) == 0) {
                LOG.warn("failed to execute {}", updateSql);
            }
        } catch (SQLException e) {
            LOG.warn("failed to execute {}, {}", updateSql, e);
            return false;
        }
        return true;
    }

    @Override
    public boolean deleteUserEmailMeta(String siteId, String userId) {
        UserEmailEntity entity = new UserEmailEntity();
        entity.setSiteId(siteId);
        entity.setUserId(userId);
        try {
            queryService.update(deleteUserEmailSql, entity, new UserEmailEntityToRelation());
        } catch (SQLException e) {
            LOG.error("Error to delete UserEmailEntity: {}", entity, e);
            return false;
        }

        return true;
    }

    @Override
    public List<UserEmailEntity> getUserEmailMeta(String siteId, String userId) {
        UserEmailEntity userEmailEntity = new UserEmailEntity();
        userEmailEntity.setSiteId(siteId);
        userEmailEntity.setUserId(userId);

        List<UserEmailEntity> results;
        try {
            results = queryService.queryWithCond(getUserEmailSql, userEmailEntity, new UserEmailEntityToRelation(), new RelationToUserEmailEntity());
        } catch (SQLException e) {
            LOG.error("Error to getJobMeta : {}", e);
            return null;
        }
        if (results.isEmpty()) {
            return null;
        }

        return results;
    }
}
