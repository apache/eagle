/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.eagle.metadata.store.jdbc.service;

import com.google.common.base.Preconditions;
import org.apache.eagle.common.function.ThrowableConsumer2;
import org.apache.eagle.common.function.ThrowableFunction;
import org.apache.eagle.metadata.exceptions.EntityNotFoundException;
import org.apache.eagle.metadata.model.DashboardEntity;
import org.apache.eagle.metadata.service.DashboardEntityService;
import org.apache.eagle.metadata.store.jdbc.JDBCMetadataQueryService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Collections;

public class DashboardEntityServiceJDBCImpl implements DashboardEntityService {
    private static final Logger LOGGER = LoggerFactory.getLogger(DashboardEntityServiceJDBCImpl.class);

    private static final String TABLE_NAME = "dashboards";
    private static final String SELECT_ALL_SQL = "SELECT * FROM " + TABLE_NAME;
    private static final String FILTER_BY_UUID_SQL = "SELECT * FROM " + TABLE_NAME + " WHERE uuid = ?";
    private static final String FILTER_BY_UUID_OR_NAME_SQL = "SELECT * FROM " + TABLE_NAME + " WHERE uuid = ? or name = ?";
    private static final String CREATE_SQL = "INSERT INTO " + TABLE_NAME + "(uuid, name, description, author, settings, charts) VALUES (?,?,?,?,?,?)";

    private static ThrowableFunction<ResultSet, DashboardEntity, SQLException> DASHBOARD_DESERIALIZER = resultSet -> {
        throw new IllegalStateException("TODO: NIY");
    };

    private static ThrowableConsumer2<PreparedStatement, DashboardEntity, SQLException> DASHBOARD_SERIALIZER = (statement, dashboard) -> {
        statement.setString(0, dashboard.getUuid());
        throw new IllegalStateException("TODO: NIY");
    };

    @Inject
    JDBCMetadataQueryService queryService;

    @Override
    public Collection<DashboardEntity> findAll() {
        try {
            return queryService.query(SELECT_ALL_SQL, DASHBOARD_DESERIALIZER);
        } catch (SQLException e) {
            LOGGER.error("Error to execute {}: {}", SELECT_ALL_SQL, e.getMessage(), e);
            throw new IllegalStateException("SQL execution error" + e.getMessage(), e);
        }
    }

    @Override
    public DashboardEntity getByUUID(String uuid) throws EntityNotFoundException {
        try {
            return queryService.queryWithCond(FILTER_BY_UUID_SQL,
                o -> o.setString(0, uuid), DASHBOARD_DESERIALIZER).stream().findAny().orElseThrow(() -> new EntityNotFoundException("Dashboard (uuid: " + uuid + ") not found"));
        } catch (SQLException e) {
            LOGGER.error("Error to execute {} (uuid: {}): {}", FILTER_BY_UUID_SQL, uuid, e.getMessage(), e);
            throw new IllegalStateException("SQL execution error:" + e.getMessage(), e);
        }
    }

    @Override
    public DashboardEntity create(DashboardEntity entity) {
        Preconditions.checkNotNull(entity, "Entity should not be null");
        entity.ensureDefault();
        try {
            int retCode = queryService.insert(CREATE_SQL, Collections.singletonList(entity), DASHBOARD_SERIALIZER);
            if (retCode > 0) {
                return entity;
            } else {
                throw new SQLException("Insertion returned: " + retCode);
            }
        } catch (SQLException e) {
            LOGGER.error("Error to insert entity {} (entity: {}): {}", CREATE_SQL, entity.toString(), e.getMessage(), e);
            throw new IllegalStateException("SQL execution error:" + e.getMessage(), e);
        }
    }

    @Override
    public DashboardEntity update(DashboardEntity entity) throws EntityNotFoundException {
        Preconditions.checkNotNull(entity, "Entity should not be null");
        Preconditions.checkNotNull(entity.getUuid(), "uuid should not be null");
        throw new IllegalStateException("TODO: NIY");
    }

    @Override
    public DashboardEntity getByUUIDOrName(String uuid, String name) throws EntityNotFoundException {
        Preconditions.checkArgument(uuid != null && name != null, "Both uuid and name are null");
        try {
            return queryService.queryWithCond(FILTER_BY_UUID_OR_NAME_SQL, o -> {
                    o.setString(0, uuid);
                    o.setString(1, name);
                }, DASHBOARD_DESERIALIZER).stream().findAny().orElseThrow(() -> new EntityNotFoundException("Dashboard (uuid: " + uuid + " or name: " + name + ") not found"));
        } catch (SQLException e) {
            LOGGER.error("Error to execute {} (uuid: {}, name: {}): {}", FILTER_BY_UUID_OR_NAME_SQL, uuid, e.getMessage(), e);
            throw new IllegalStateException("SQL execution error:" + e.getMessage(), e);
        }
    }

    @Override
    public DashboardEntity deleteByUUID(String uuid) throws EntityNotFoundException {
        Preconditions.checkNotNull(uuid, "uuid should not be null");
        throw new IllegalStateException("TODO: NIY");
    }
}