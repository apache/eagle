/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.eagle.metadata.store.jdbc.service;

import com.fasterxml.jackson.databind.ObjectMapper;

import com.google.common.base.Preconditions;
import org.apache.eagle.alert.engine.coordinator.PolicyDefinition;
import org.apache.eagle.common.function.ThrowableConsumer2;
import org.apache.eagle.common.function.ThrowableFunction;
import org.apache.eagle.metadata.exceptions.EntityNotFoundException;
import org.apache.eagle.metadata.model.PolicyEntity;
import org.apache.eagle.metadata.service.PolicyEntityService;
import org.apache.eagle.metadata.store.jdbc.JDBCMetadataQueryService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

public class PolicyEntityServiceJDBCImpl implements PolicyEntityService {
    private static final Logger LOGGER = LoggerFactory.getLogger(PolicyEntityServiceJDBCImpl.class);

    private static final String selectSql = "SELECT * FROM policy_prototype";
    private static final String queryByUUID = "SELECT * FROM policy_prototype where uuid = '%s'";
    private static final String deleteSqlByUUID = "DELETE FROM policy_prototype where uuid = '%s'";
    private static final String updateSqlByUUID = "UPDATE policy_prototype SET name = ?, definition = ? , alertPublisherIds = ? , createdtime = ? , modifiedtime = ?  where uuid = ?";
    private static final String insertSql = "INSERT INTO policy_prototype (name, definition, alertPublisherIds, createdtime, modifiedtime, uuid) VALUES (?, ?, ?, ?, ?, ?)";

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    @Inject
    JDBCMetadataQueryService queryService;

    @Override
    public Collection<PolicyEntity> getAllPolicyProto() {
        try {
            return queryService.query(selectSql, policyEntityMapper);
        } catch (Exception e) {
            throw new IllegalArgumentException(e.getMessage(), e);
        }
    }

    @Override
    public PolicyEntity getPolicyProtoByUUID(String uuid) {
        Preconditions.checkNotNull(uuid, "uuid should not be null");
        try {
            return queryService.query(String.format(queryByUUID, uuid), policyEntityMapper).stream()
                    .findAny().orElseThrow(() -> new EntityNotFoundException("policyProto is not found by uuid"));
        } catch (Exception e) {
            throw new IllegalArgumentException(e.getMessage(), e);
        }
    }

    @Override
    public boolean deletePolicyProtoByUUID(String uuid) {
        String sql = String.format(deleteSqlByUUID, uuid);
        try {
            return queryService.execute(sql);
        } catch (Exception e) {
            LOGGER.error("Error to execute {}: {}", sql, e);
            throw new IllegalArgumentException("SQL execution error: " + e.getMessage(), e);
        }
    }

    @Override
    public PolicyEntity update(PolicyEntity policyProto) {
        Preconditions.checkNotNull(policyProto, "Entity should not be null");
        Preconditions.checkNotNull(policyProto.getUuid(), "uuid should not be null");
        PolicyEntity current = getPolicyProtoByUUID(policyProto.getUuid());

        if (policyProto.getName() != null) {
            current.setName(policyProto.getName());
        }
        if (policyProto.getAlertPublishmentIds() != null) {
            current.setAlertPublishmentIds(policyProto.getAlertPublishmentIds());
        }
        if (policyProto.getDefinition() != null) {
            current.setDefinition(policyProto.getDefinition());
        }
        current.ensureDefault();

        try {
            if (!queryService.execute(updateSqlByUUID, current, policyEntityWriter)) {
                throw new IllegalArgumentException("Failed to update policyProto");
            }
        } catch (SQLException e) {
            LOGGER.error("Error to execute {}: {}", updateSqlByUUID, policyProto, e);
            throw new IllegalArgumentException("SQL execution error: " + e.getMessage(), e);
        }
        return current;
    }



    @Override
    public PolicyEntity create(PolicyEntity entity) {
        Preconditions.checkNotNull(entity, "PolicyEntity should not be null");
        entity.ensureDefault();
        try {
            int retCode = queryService.insert(insertSql, Collections.singletonList(entity), policyEntityWriter);
            if (retCode > 0) {
                return entity;
            } else {
                throw new SQLException("Insertion result: " + retCode);
            }
        } catch (SQLException e) {
            LOGGER.error("Error to insert entity {} (entity: {}): {}", insertSql, entity.toString(), e.getMessage(), e);
            throw new IllegalArgumentException("SQL execution error:" + e.getMessage(), e);
        }
    }

    private ThrowableFunction<ResultSet, PolicyEntity, SQLException> policyEntityMapper = resultSet -> {
        PolicyEntity entity = new PolicyEntity();
        entity.setName(resultSet.getString("name"));
        entity.setUuid(resultSet.getString("uuid"));
        String policyStr = resultSet.getString("definition");
        if (policyStr != null) {
            try {
                PolicyDefinition policyDefinition = OBJECT_MAPPER.readValue(policyStr, PolicyDefinition.class);
                entity.setDefinition(policyDefinition);
            } catch (Exception e) {
                throw new SQLException("Error to deserialize JSON as {}", PolicyDefinition.class.getCanonicalName(), e);
            }
        }
        String list = resultSet.getString("alertPublisherIds");
        if (list != null) {
            try {
                List<String> alertPublisherIds = OBJECT_MAPPER.readValue(list, List.class);
                entity.setAlertPublishmentIds(alertPublisherIds);
            } catch (Exception e) {
                throw new SQLException("Error to deserialize JSON as AlertPublisherIds list", e);
            }
        }
        entity.setCreatedTime(resultSet.getLong("createdtime"));
        entity.setModifiedTime(resultSet.getLong("modifiedtime"));
        return entity;
    };

    private ThrowableConsumer2<PreparedStatement, PolicyEntity, SQLException> policyEntityWriter = (statement, policyEntity) -> {
        policyEntity.ensureDefault();

        statement.setString(1, policyEntity.getName());
        if (policyEntity.getDefinition() != null) {
            try {
                statement.setString(2, OBJECT_MAPPER.writeValueAsString(policyEntity.getDefinition()));
            } catch (Exception e) {
                throw new IllegalArgumentException(e.getMessage(), e);
            }
        }
        if (policyEntity.getAlertPublishmentIds() != null) {
            try {
                statement.setString(3, OBJECT_MAPPER.writeValueAsString(policyEntity.getAlertPublishmentIds()));
            } catch (Exception e) {
                throw new IllegalArgumentException(e.getMessage(), e);
            }
        }
        statement.setLong(4, policyEntity.getCreatedTime());
        statement.setLong(5, policyEntity.getModifiedTime());
        statement.setString(6, policyEntity.getUuid());
    };


}

