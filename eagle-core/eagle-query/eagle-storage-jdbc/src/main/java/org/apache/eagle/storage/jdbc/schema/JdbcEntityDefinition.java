/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.eagle.storage.jdbc.schema;

import org.apache.eagle.log.entity.GenericMetricEntity;
import org.apache.eagle.log.entity.meta.EntityDefinition;
import org.apache.eagle.log.entity.meta.Qualifier;
import org.apache.eagle.storage.jdbc.JdbcConstants;
import org.apache.eagle.storage.jdbc.schema.serializer.JdbcSerDeser;

import java.util.Map;

public class JdbcEntityDefinition {
    private final EntityDefinition internal;

    public JdbcEntityDefinition(EntityDefinition internal) {
        this.internal = internal;
    }

    /**
     * get internal entity definition.
     * @return internal entity definition
     */
    public EntityDefinition getInternal() {
        return this.internal;
    }

    /**
     * As to GenericMetricEntity, return "${tableName}", else return "${tableName}_${prefix}".
     *
     * @return jdbc table name in lowercase
     */
    public String getJdbcTableName() {
        if (isGenericMetric()) {
            return this.internal.getTable().toLowerCase();
        } else {
            return String.format("%s_%s", this.internal.getTable(), this.internal.getPrefix()).toLowerCase();
        }
    }

    public Class<?> getColumnType(String fieldName) throws NoSuchFieldException {
        if (fieldName.equalsIgnoreCase(JdbcConstants.TIMESTAMP_COLUMN_NAME)) {
            return Long.class;
        } else if (fieldName.equalsIgnoreCase(JdbcConstants.ROW_KEY_COLUMN_NAME)) {
            return String.class;
        } else if (fieldName.equalsIgnoreCase(JdbcConstants.METRIC_NAME_COLUMN_NAME)) {
            return String.class;
        }
        for (String realField : internal.getDisplayNameMap().keySet()) {
            if (realField.equalsIgnoreCase(fieldName)) {
                return internal.getEntityClass().getDeclaredField(realField).getType();
            }
        }
        throw new NoSuchFieldException(fieldName);
    }

    public Class<?> getColumnTypeOrNull(String fieldName) {
        try {
            return getColumnType(fieldName);
        } catch (NoSuchFieldException e) {
            return null;
        }
    }

    public Integer getJdbcColumnTypeCodeOrNull(String fieldName) {
        if (this.isGenericMetric() && GenericMetricEntity.VALUE_FIELD.equalsIgnoreCase(fieldName)) {
            return JdbcEntityDefinitionManager.getJdbcType(double.class);
        }
        Class<?> columnType;
        try {
            columnType = getColumnType(fieldName);
            return JdbcEntityDefinitionManager.getJdbcType(columnType);
        } catch (NoSuchFieldException e) {
            return null;
        }
    }

    @SuppressWarnings("unchecked")
    public JdbcSerDeser getJdbcSerDeser(String columnName) {
        if (this.isGenericMetric() && GenericMetricEntity.VALUE_FIELD.equalsIgnoreCase(columnName)) {
            return JdbcEntityDefinitionManager.METRIC_JDBC_SERDESER;
        }

        Qualifier qualifier = this.getColumnQualifier(columnName);
        if (qualifier == null) {
            return JdbcEntityDefinitionManager.DEFAULT_JDBC_SERDESER;
        } else {
            return JdbcEntityDefinitionManager.getJdbcSerDeser(qualifier.getSerDeser().getClass());
        }
    }

    public boolean isGenericMetric() {
        return this.internal.getEntityClass().equals(GenericMetricEntity.class);
    }

    public boolean isField(String columnName) {
        for (String name : this.internal.getDisplayNameMap().keySet()) {
            if (name.equalsIgnoreCase(columnName)) {
                return true;
            }
        }
        return false;
    }

    /**
     * TODO: Optimize with hashmap.
     */
    public String getJavaEntityFieldName(String jdbcColumnName) {
        for (String javaEntityFieldName : this.internal.getDisplayNameMap().keySet()) {
            if (javaEntityFieldName.equalsIgnoreCase(jdbcColumnName)) {
                return javaEntityFieldName;
            }
        }
        throw new IllegalArgumentException("Can't map jdbc column '" + jdbcColumnName + "' with entity: " + this.getInternal().getEntityClass());
    }

    /**
     * TODO: Optimize with hashmap.
     *
     * @param jdbcTagName
     * @return
     */
    public String getOriginalJavaTagName(String jdbcTagName) {
        for (String tag : this.getInternal().getTags()) {
            if (tag.equalsIgnoreCase(jdbcTagName)) {
                return tag;
            }
        }
        return jdbcTagName.toLowerCase();
    }

    public Qualifier getColumnQualifier(String columnName) {
        for (Map.Entry<String, Qualifier> entry : this.internal.getDisplayNameMap().entrySet()) {
            if (entry.getKey().equalsIgnoreCase(columnName)) {
                return entry.getValue();
            }
        }
        return null;
    }
}