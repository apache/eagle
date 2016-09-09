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


import org.apache.eagle.metadata.model.ApplicationEntity;
import org.apache.eagle.metadata.service.ApplicationEntityService;
import org.apache.eagle.metadata.store.jdbc.JDBCMetadataQueryService;
import org.apache.eagle.metadata.store.jdbc.service.orm.ApplicationEntityToRelation;
import org.apache.eagle.metadata.store.jdbc.service.orm.RelationToApplicationEntity;

import com.google.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import javax.inject.Inject;

@Singleton
public class ApplicationEntityServiceJDBCImpl implements ApplicationEntityService {

    private static final Logger LOGGER = LoggerFactory.getLogger(ApplicationEntityServiceJDBCImpl.class);

    private static final String insertSql = "INSERT INTO applicationentity (siteid, apptype, appmode, jarpath, appstatus, createdtime, modifiedtime, uuid, appid ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)";
    private static final String selectSql = "SELECT * FROM applicationentity a INNER JOIN siteentity s on  a.siteid = s.siteid";
    private static final String selectSqlBySiteIdAndAppType = "SELECT * FROM applicationentity  a INNER JOIN siteentity s on  a.siteid = s.siteid where a.siteid = ? and a.apptype = ?";
    private static final String selectSqlBySiteId = "SELECT * FROM applicationentity  a INNER JOIN siteentity s on  a.siteid = s.siteid where a.siteid = ?";
    private static final String selectSqlByUUId = "SELECT * FROM applicationentity  a INNER JOIN siteentity s on  a.siteid = s.siteid where a.uuid = ?";
    private static final String selectSqlByAppId = "SELECT * FROM applicationentity  a INNER JOIN siteentity s on  a.siteid = s.siteid where a.appid = ?";
    private static final String deleteSqlByUUID = "DELETE FROM applicationentity where uuid = ?";

    @Inject
    JDBCMetadataQueryService queryService;

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
    public Collection<ApplicationEntity> findAll() {
        List<ApplicationEntity> results = new ArrayList<>();
        try {
            results = queryService.query(selectSql, new RelationToApplicationEntity());
        } catch (SQLException e) {
            LOGGER.error("Error to findAll ApplicationEntity: {}", e);
        }
        return results;
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
