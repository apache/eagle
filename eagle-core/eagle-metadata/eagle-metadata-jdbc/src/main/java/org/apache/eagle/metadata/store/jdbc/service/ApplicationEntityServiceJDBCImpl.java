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

package org.apache.eagle.metadata.store.jdbc.service;


import com.google.inject.Singleton;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.commons.lang.StringUtils;
import org.apache.eagle.alert.engine.coordinator.StreamDefinition;
import org.apache.eagle.app.environment.ExecutionRuntime;
import org.apache.eagle.app.environment.ExecutionRuntimeManager;
import org.apache.eagle.app.service.ApplicationProviderService;
import org.apache.eagle.metadata.model.ApplicationEntity;
import org.apache.eagle.metadata.model.StreamDesc;
import org.apache.eagle.metadata.model.StreamSinkConfig;
import org.apache.eagle.metadata.service.ApplicationEntityService;
import org.apache.eagle.metadata.store.jdbc.JDBCMetadataQueryService;
import org.apache.eagle.metadata.store.jdbc.service.orm.ApplicationEntityToRelation;
import org.apache.eagle.metadata.store.jdbc.service.orm.RelationToApplicationEntity;
import org.apache.eagle.metadata.utils.StreamIdConversions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

@Singleton
public class ApplicationEntityServiceJDBCImpl implements ApplicationEntityService {

    private static final Logger LOGGER = LoggerFactory.getLogger(ApplicationEntityServiceJDBCImpl.class);

    private static final String insertSql = "INSERT INTO applications (siteid, apptype, appmode, jarpath, appstatus, configuration, context, createdtime, modifiedtime, uuid, appid ) VALUES (?, ?, "
        + "?, ?, ?, ?, ?, ?, ?, ?, ?)";
    private static final String selectSql = "SELECT * FROM applications a INNER JOIN sites s on  a.siteid = s.siteid";
    private static final String selectSqlBySiteIdAndAppType = "SELECT * FROM applications  a INNER JOIN sites s on  a.siteid = s.siteid where a.siteid = ? and a.apptype = ?";
    private static final String selectSqlBySiteId = "SELECT * FROM applications  a INNER JOIN sites s on  a.siteid = s.siteid where a.siteid = ?";
    private static final String selectSqlByUUId = "SELECT * FROM applications  a INNER JOIN sites s on  a.siteid = s.siteid where a.uuid = ?";
    private static final String selectSqlByAppId = "SELECT * FROM applications  a INNER JOIN sites s on  a.siteid = s.siteid where a.appid = ?";
    private static final String deleteSqlByUUID = "DELETE FROM applications where uuid = ?";

    @Inject
    JDBCMetadataQueryService queryService;
    @Inject
    ApplicationProviderService applicationProviderService;
    @Inject
    Config config;

    @Override
    public Collection<ApplicationEntity> findBySiteId(String siteId) {
        ApplicationEntity applicationEntity = new ApplicationEntity(siteId, "");
        List<ApplicationEntity> results = new ArrayList<>();
        try {
            results = queryService.queryWithCond(selectSqlBySiteId, applicationEntity, new ApplicationEntityToRelation(), new RelationToApplicationEntity());
        } catch (SQLException e) {
            LOGGER.error("Error to getBySiteIdAndAppType ApplicationEntity: {}", e);
            return results;
        }
        fillApplicationDesc(results);
        return results;
    }

    @Override
    public ApplicationEntity getBySiteIdAndAppType(String siteId, String appType) {

        ApplicationEntity applicationEntity = new ApplicationEntity(siteId, appType);

        List<ApplicationEntity> results;
        try {
            results = queryService.queryWithCond(selectSqlBySiteIdAndAppType, applicationEntity, new ApplicationEntityToRelation(), new RelationToApplicationEntity());
        } catch (SQLException e) {
            LOGGER.error("Error to getBySiteIdAndAppType ApplicationEntity: {}", e);
            return null;
        }
        if (results.isEmpty()) {
            return null;
        }

        fillApplicationDesc(results);
        return results.get(0);
    }

    @Override
    public ApplicationEntity getByUUIDOrAppId(String uuid, String appId) {
        if (uuid == null && appId == null) {
            throw new IllegalArgumentException("uuid and appId are both null");
        }
        if (uuid != null) {
            return getByUUID(uuid);
        }
        ApplicationEntity applicationEntity = new ApplicationEntity(null, null, null, null, "", appId);
        List<ApplicationEntity> results = new ArrayList<>(1);
        try {
            results = queryService.queryWithCond(selectSqlByAppId, applicationEntity, new ApplicationEntityToRelation(), new RelationToApplicationEntity());
        } catch (SQLException e) {
            LOGGER.error("Error to findAll ApplicationEntity: {}", e);
        }
        if (results.isEmpty()) {
            throw new IllegalArgumentException("Application with appId: " + appId + " not found");
        }
        fillApplicationDesc(results);
        return results.get(0);
    }

    @Override
    public ApplicationEntity delete(ApplicationEntity applicationEntity) {
        ApplicationEntity entity = getByUUIDOrAppId(applicationEntity.getUuid(), applicationEntity.getAppId());
        entity = new ApplicationEntity(null, null, null, null, entity.getUuid(), "");
        try {
            queryService.update(deleteSqlByUUID, entity, new ApplicationEntityToRelation());
        } catch (SQLException e) {
            LOGGER.error("Error to delete ApplicationEntity: {}", entity, e);
        }
        return entity;
    }

    @Override
    public ApplicationEntity update(ApplicationEntity entity) {
        String updateSql = "update applications set ";
        if (entity.getSite() != null && StringUtils.isNotBlank(entity.getSite().getSiteId())) {
            updateSql += "siteid = ?, ";
        }
        if (entity.getDescriptor() != null && StringUtils.isNotBlank(entity.getDescriptor().getType())) {
            updateSql += "apptype = ?, ";
        }
        if (entity.getMode() != null && StringUtils.isNotBlank(entity.getMode().name())) {
            updateSql += "appmode = ?, ";
        }
        if (StringUtils.isNotBlank(entity.getJarPath())) {
            updateSql += "jarpath = ?, ";
        }
        if (entity.getStatus() != null && StringUtils.isNotBlank(entity.getStatus().name())) {
            updateSql += "appstatus = ?, ";
        }
        if (entity.getConfiguration() != null && !entity.getConfiguration().isEmpty()) {
            updateSql += "configuration = ?, ";
        }
        if (entity.getContext() != null && !entity.getContext().isEmpty()) {
            updateSql += "context = ?, ";
        }
        if (entity.getCreatedTime() > 0) {
            updateSql += "createdtime = ?, ";
        }
        if (entity.getModifiedTime() > 0) {
            updateSql += "modifiedtime = ?, ";
        }
        updateSql = updateSql.substring(0, updateSql.length() - 2);
        if (StringUtils.isNotBlank(entity.getUuid())) {
            updateSql += " where uuid = ?";
        }
        if (StringUtils.isNotBlank(entity.getAppId())) {
            updateSql += " and appid = ?";
        }

        try {
            if (queryService.update(updateSql, entity, new ApplicationEntityToRelation()) == 0) {
                LOGGER.warn("failed to execute {}", updateSql);
            }
        } catch (SQLException e) {
            LOGGER.warn("failed to execute {}, {}", updateSql, e);
        }
        return getByUUID(entity.getUuid());
    }

    @Override
    public Collection<ApplicationEntity> findAll() {
        List<ApplicationEntity> results = new ArrayList<>();
        try {
            results = queryService.query(selectSql, new RelationToApplicationEntity());
        } catch (SQLException e) {
            LOGGER.error("Error to findAll ApplicationEntity: {}", e);
        }
        fillApplicationDesc(results);
        return results;
    }

    private void fillApplicationDesc(List<ApplicationEntity> entities) {
        for (ApplicationEntity entity : entities) {
            entity.setDescriptor(applicationProviderService.getApplicationDescByType(entity.getDescriptor().getType()));
            if (entity.getDescriptor().getStreams() == null) {
                continue;
            }
            List<StreamDesc> streamDescToInstall = entity.getDescriptor().getStreams().stream().map((streamDefinition -> {
                StreamDefinition copied = streamDefinition.copy();
                copied.setSiteId(entity.getSite().getSiteId());
                copied.setStreamId(StreamIdConversions.formatSiteStreamId(entity.getSite().getSiteId(), copied.getStreamId()));
                Config effectiveConfig = ConfigFactory.parseMap(new HashMap<>(entity.getConfiguration()))
                    .withFallback(config).withFallback(ConfigFactory.parseMap(entity.getContext()));

                ExecutionRuntime runtime = ExecutionRuntimeManager.getInstance().getRuntimeSingleton(
                    applicationProviderService.getApplicationProviderByType(entity.getDescriptor().getType()).getApplication().getEnvironmentType(), config);
                StreamSinkConfig streamSinkConfig = runtime.environment()
                    .stream().getSinkConfig(StreamIdConversions.parseStreamTypeId(copied.getSiteId(), copied.getStreamId()), effectiveConfig);
                StreamDesc streamDesc = new StreamDesc();
                streamDesc.setSchema(copied);
                streamDesc.setSinkConfig(streamSinkConfig);
                streamDesc.setStreamId(copied.getStreamId());
                streamDesc.getSchema().setDataSource(entity.getAppId());
                return streamDesc;
            })).collect(Collectors.toList());
            entity.setStreams(streamDescToInstall);
        }
    }

    @Override
    public ApplicationEntity getByUUID(String uuid) {
        ApplicationEntity applicationEntity = new ApplicationEntity(null, null, null, null, uuid, "");
        List<ApplicationEntity> results = new ArrayList<>(1);
        try {
            results = queryService.queryWithCond(selectSqlByUUId, applicationEntity, new ApplicationEntityToRelation(), new RelationToApplicationEntity());
        } catch (SQLException e) {
            LOGGER.error("Error to findAll ApplicationEntity: {}", e);
        }
        if (results.isEmpty()) {
            throw new IllegalArgumentException("Application (UUID: " + uuid + ") is not found");
        }
        fillApplicationDesc(results);
        return results.get(0);
    }

    @Override
    public ApplicationEntity create(ApplicationEntity entity) {
        entity.ensureDefault();
        if (getBySiteIdAndAppType(entity.getSite().getSiteId(), entity.getDescriptor().getType()) != null) {
            throw new IllegalArgumentException("Duplicated appId: " + entity.getAppId());
        }

        List<ApplicationEntity> entities = new ArrayList<>(1);
        entities.add(entity);
        try {
            queryService.insert(insertSql, entities, new ApplicationEntityToRelation());
        } catch (SQLException e) {
            LOGGER.error("Error to insert ApplicationEntity: {}", entity, e);
        }
        return entity;
    }
}
