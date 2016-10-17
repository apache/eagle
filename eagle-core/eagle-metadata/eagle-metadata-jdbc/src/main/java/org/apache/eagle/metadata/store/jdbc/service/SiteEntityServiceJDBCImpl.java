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

import org.apache.eagle.metadata.exceptions.EntityNotFoundException;
import org.apache.eagle.metadata.model.SiteEntity;
import org.apache.eagle.metadata.service.SiteEntityService;
import org.apache.eagle.metadata.store.jdbc.JDBCMetadataQueryService;
import org.apache.eagle.metadata.store.jdbc.service.orm.RelationToSiteEntity;
import org.apache.eagle.metadata.store.jdbc.service.orm.SiteEntityToRelation;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import javax.inject.Inject;


public class SiteEntityServiceJDBCImpl implements SiteEntityService {

    private static final Logger LOGGER = LoggerFactory.getLogger(SiteEntityServiceJDBCImpl.class);
    private static final String insertSql = "INSERT INTO sites (siteid, sitename, description, createdtime, modifiedtime, uuid) VALUES (?, ?, ?, ?, ?, ?)";
    private static final String selectSql = "SELECT * FROM sites";
    private static final String selectSqlByUUID = "SELECT * FROM sites where uuid = ?";
    private static final String selectSqlBySiteId = "SELECT * FROM sites where siteid = ?";
    private static final String deleteSqlByUUID = "DELETE FROM sites where uuid = ?";
    private static final String deleteSqlBySiteId = "DELETE FROM sites where siteid = ?";
    private static final String updateSqlByUUID = "UPDATE sites SET siteid = ? , sitename = ? , description = ? , createdtime = ? , modifiedtime = ?  where uuid = ?";
    @Inject
    JDBCMetadataQueryService queryService;

    @Override
    public SiteEntity getBySiteId(String siteId) throws EntityNotFoundException {
        List<SiteEntity> results;
        SiteEntity siteEntity = new SiteEntity("", siteId);
        try {
            results = queryService.queryWithCond(selectSqlBySiteId, siteEntity, new SiteEntityToRelation(), new RelationToSiteEntity());
        } catch (SQLException e) {
            LOGGER.error("Error to getBySiteId SiteEntity: {}", e);
            throw new EntityNotFoundException(e);
        }
        if (results.isEmpty()) {
            throw new EntityNotFoundException("getBySiteId " + siteId + " Not Found");
        }
        return results.get(0);
    }

    @Override
    public SiteEntity deleteBySiteId(String siteId) throws EntityNotFoundException {
        SiteEntity siteEntity = new SiteEntity("", siteId);
        int result;
        try {
            result = queryService.update(deleteSqlBySiteId, siteEntity, new SiteEntityToRelation());
        } catch (SQLException e) {
            LOGGER.error("Error to deleteBySiteId SiteEntity: {}", siteEntity, e);
            throw new EntityNotFoundException(e);
        }
        if (result == 0) {
            throw new EntityNotFoundException("deleteBySiteId " + siteEntity + "Not Found");
        }
        return siteEntity;
    }

    @Override
    public SiteEntity deleteByUUID(String uuid) throws EntityNotFoundException {
        SiteEntity siteEntity = new SiteEntity(uuid, "");
        int result;
        try {
            result = queryService.update(deleteSqlByUUID, siteEntity, new SiteEntityToRelation());
        } catch (SQLException e) {
            LOGGER.error("Error to deleteByUUID SiteEntity: {}", siteEntity, e);
            throw new EntityNotFoundException(e);
        }
        if (result == 0) {
            throw new EntityNotFoundException("deleteByUUID " + siteEntity + "Not Found");
        }
        return siteEntity;
    }

    @Override
    public SiteEntity update(SiteEntity siteEntity) throws EntityNotFoundException {

        if (siteEntity.getSiteId() == null && siteEntity.getUuid() == null) {
            throw new IllegalArgumentException("siteId and UUID are both null, don't know how to update");
        }
        int result;
        try {
            SiteEntity oldEntity = getBySiteId(siteEntity.getSiteId());
            siteEntity.setUuid(oldEntity.getUuid());
            siteEntity.setCreatedTime(oldEntity.getCreatedTime());
            siteEntity.ensureDefault();
            result = queryService.update(updateSqlByUUID, siteEntity, new SiteEntityToRelation());
        } catch (SQLException e) {
            LOGGER.error("Error to update SiteEntity: {}", siteEntity, e);
            throw new EntityNotFoundException(e);
        }
        if (result == 0) {
            throw new EntityNotFoundException("update " + siteEntity + "Not Found");
        }

        return siteEntity;
    }

    @Override
    public Collection<SiteEntity> findAll() {
        List<SiteEntity> results = new ArrayList<>();
        try {
            results = queryService.query(selectSql, new RelationToSiteEntity());
        } catch (SQLException e) {
            LOGGER.error("Error to findAll SiteEntity: {}", e);
        }
        return results;
    }

    @Override
    public SiteEntity getByUUID(String uuid) throws EntityNotFoundException {
        List<SiteEntity> results;
        SiteEntity siteEntity = new SiteEntity(uuid, "");
        try {
            results = queryService.queryWithCond(selectSqlByUUID, siteEntity, new SiteEntityToRelation(), new RelationToSiteEntity());
        } catch (SQLException e) {
            LOGGER.error("Error to getByUUID SiteEntity: {}", e);
            throw new EntityNotFoundException(e);
        }
        if (results.isEmpty()) {
            throw new EntityNotFoundException("getByUUID " + uuid + " Not Found");
        }
        return results.get(0);
    }

    @Override
    public SiteEntity create(SiteEntity entity) {
        Preconditions.checkNotNull(entity.getSiteId(), "SiteId is null: " + entity.getSiteId());
        List<SiteEntity> entities = new ArrayList<>(1);
        entity.ensureDefault();
        entities.add(entity);
        try {
            queryService.insert(insertSql, entities, new SiteEntityToRelation());
        } catch (SQLException e) {
            LOGGER.error("Error to insert SiteEntity: {}", entity, e);
            throw new IllegalArgumentException("Error to insert SiteEntity: " + entity + e);
        }
        return entity;
    }
}
