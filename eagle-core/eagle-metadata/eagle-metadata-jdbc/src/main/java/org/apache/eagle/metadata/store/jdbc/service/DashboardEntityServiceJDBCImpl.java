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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.type.CollectionType;
import com.fasterxml.jackson.databind.type.MapType;
import com.fasterxml.jackson.databind.type.TypeFactory;
import com.google.common.base.Preconditions;
import com.mongodb.util.JSON;
import org.apache.eagle.common.function.ThrowableConsumer2;
import org.apache.eagle.common.function.ThrowableFunction;
import org.apache.eagle.common.security.User;
import org.apache.eagle.metadata.exceptions.EntityNotFoundException;
import org.apache.eagle.metadata.model.DashboardEntity;
import org.apache.eagle.metadata.service.DashboardEntityService;
import org.apache.eagle.metadata.store.jdbc.JDBCMetadataQueryService;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

public class DashboardEntityServiceJDBCImpl implements DashboardEntityService {
    private static final Logger LOGGER = LoggerFactory.getLogger(DashboardEntityServiceJDBCImpl.class);

    private static final String TABLE_NAME = "dashboards";
    private static final String SELECT_ALL_SQL = "SELECT * FROM " + TABLE_NAME;
    private static final String FILTER_BY_UUID_SQL = "SELECT * FROM " + TABLE_NAME + " WHERE uuid = ?";
    private static final String DELETE_BY_UUID_SQL_FORMAT = "DELETE FROM " + TABLE_NAME + " WHERE uuid = '%s'";
    private static final String FILTER_BY_UUID_OR_NAME_SQL = "SELECT * FROM " + TABLE_NAME + " WHERE uuid = ? or name = ?";
    private static final String CREATE_SQL = "INSERT INTO " + TABLE_NAME
        + "(name, description, author, charts, settings, modifiedtime, createdtime, uuid) VALUES (?,?,?,?,?,?,?,?)";
    private static final String UPDATE_SQL = "UPDATE " + TABLE_NAME
        + " SET name = ?, description = ?, author = ?, charts = ?, settings = ?, modifiedtime = ?, createdtime = ? WHERE uuid = ?";

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final MapType SETTINGS_TYPE = TypeFactory.defaultInstance().constructMapType(HashMap.class, String.class, Object.class);
    private static final CollectionType CHARTS_TYPE = TypeFactory.defaultInstance().constructCollectionType(LinkedList.class, String.class);

    private static ThrowableFunction<ResultSet, DashboardEntity, SQLException> DASHBOARD_DESERIALIZER = resultSet -> {
        DashboardEntity entity = new DashboardEntity();
        entity.setUuid(resultSet.getString("uuid"));
        entity.setName(resultSet.getString("name"));
        entity.setDescription(resultSet.getString("description"));
        String settings = resultSet.getString("settings");
        String charts = resultSet.getString("charts");
        if (charts != null) {
            try {
                entity.setCharts(OBJECT_MAPPER.readValue(charts, CHARTS_TYPE));
            } catch (IOException e) {
                throw new SQLException("Error to deserialize JSON as List<String>: " + charts, e);
            }
        }
        if (settings != null) {
            try {
                entity.setSettings(OBJECT_MAPPER.readValue(settings, SETTINGS_TYPE));
            } catch (IOException e) {
                throw new IllegalArgumentException("Error to deserialize JSON as Map<String, Object>: " + settings, e);
            }
        }
        entity.setAuthor(resultSet.getString("author"));
        entity.setCreatedTime(resultSet.getLong("createdtime"));
        entity.setModifiedTime(resultSet.getLong("modifiedtime"));
        return entity;
    };

    private static ThrowableConsumer2<PreparedStatement, DashboardEntity, SQLException> DASHBOARD_SERIALIZER = (statement, dashboard) -> {
        statement.setString(1, dashboard.getName());
        statement.setString(2, dashboard.getDescription());
        statement.setString(3, dashboard.getAuthor());
        if (dashboard.getCharts() != null) {
            try {
                statement.setString(4, OBJECT_MAPPER.writeValueAsString(dashboard.getCharts()));
            } catch (JsonProcessingException e) {
                throw new IllegalArgumentException(e.getMessage(), e);
            }
        }
        if (dashboard.getSettings() != null) {
            try {
                statement.setString(5, OBJECT_MAPPER.writeValueAsString(dashboard.getSettings()));
            } catch (JsonProcessingException e) {
                throw new IllegalArgumentException(e.getMessage(), e);
            }
        }
        statement.setLong(6, dashboard.getModifiedTime());
        statement.setLong(7, dashboard.getCreatedTime());
        statement.setString(8, dashboard.getUuid());
    };

    @Inject
    JDBCMetadataQueryService queryService;

    @Override
    public Collection<DashboardEntity> findAll() {
        try {
            return queryService.query(SELECT_ALL_SQL, DASHBOARD_DESERIALIZER);
        } catch (SQLException e) {
            LOGGER.error("Error to execute {}: {}", SELECT_ALL_SQL, e.getMessage(), e);
            throw new IllegalArgumentException("SQL execution error" + e.getMessage(), e);
        }
    }

    @Override
    public DashboardEntity getByUUID(String uuid) throws EntityNotFoundException {
        try {
            return queryService.queryWithCond(FILTER_BY_UUID_SQL,
                o -> o.setString(1, uuid), DASHBOARD_DESERIALIZER).stream().findAny().orElseThrow(() -> new EntityNotFoundException("Dashboard (uuid: " + uuid + ") not found"));
        } catch (SQLException e) {
            LOGGER.error("Error to execute {} (uuid: {}): {}", FILTER_BY_UUID_SQL, uuid, e.getMessage(), e);
            throw new IllegalArgumentException("SQL execution error:" + e.getMessage(), e);
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
            throw new IllegalArgumentException("SQL execution error:" + e.getMessage(), e);
        }
    }

    @Override
    public DashboardEntity update(DashboardEntity entity, User user) throws EntityNotFoundException {
        Preconditions.checkNotNull(entity, "Entity should not be null");
        Preconditions.checkNotNull(entity.getUuid(), "uuid should not be null");
        DashboardEntity current = getByUUID(entity.getUuid());

        Preconditions.checkArgument(user.isInRole(User.Role.ADMINISTRATOR)
            || current.getAuthor().equals(user.getName()), "UPDATE operation is not allowed");

        if (entity.getName() != null) {
            current.setName(entity.getName());
        }
        if (entity.getDescription() != null) {
            current.setDescription(entity.getDescription());
        }
        if (entity.getAuthor() != null) {
            current.setAuthor(entity.getAuthor());
        }
        if (entity.getCharts() != null) {
            current.setCharts(entity.getCharts());
        }
        if (entity.getSettings() != null) {
            current.setSettings(entity.getSettings());
        }
        if (entity.getCreatedTime() > 0) {
            LOGGER.warn("createdTime  is not updatable but provided: {}, ignore", current.getCreatedTime());
        }
        if (entity.getModifiedTime() > 0) {
            LOGGER.warn("modifiedTime is not updatable but provided: {}, ignore", current.getModifiedTime());
        }
        current.ensureDefault();
        try {
            if (!queryService.execute(UPDATE_SQL, current, DASHBOARD_SERIALIZER)) {
                throw new IllegalArgumentException("Failed to update dashboard");
            }
        } catch (SQLException e) {
            LOGGER.error("Error to execute {}: {}", UPDATE_SQL, current, e);
            throw new IllegalArgumentException("SQL execution error: " + e.getMessage(), e);
        }
        return current;
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
            throw new IllegalArgumentException("SQL execution error: " + e.getMessage(), e);
        }
    }

    @Override
    public DashboardEntity deleteByUUID(String uuid, User user) throws EntityNotFoundException {
        Preconditions.checkNotNull(uuid, "uuid should not be null");
        DashboardEntity entity = this.getByUUID(uuid);

        Preconditions.checkArgument(user.isInRole(User.Role.ADMINISTRATOR)
            || entity.getAuthor().equals(user.getName()), "DELETE operation is not allowed");

        try {
            queryService.execute(String.format(DELETE_BY_UUID_SQL_FORMAT, uuid));
        } catch (SQLException e) {
            LOGGER.error("Error to execute {}: {}", String.format(DELETE_BY_UUID_SQL_FORMAT, uuid), uuid, e.getMessage(), e);
            throw new IllegalArgumentException("SQL execution error: " + e.getMessage(), e);
        }
        return entity;
    }
}