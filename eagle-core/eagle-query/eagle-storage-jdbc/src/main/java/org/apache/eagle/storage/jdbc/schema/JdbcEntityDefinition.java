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
import org.apache.eagle.storage.jdbc.schema.serializer.JdbcSerDeser;

/**
 * @since 3/26/15
 */
public class JdbcEntityDefinition {
    private final EntityDefinition internal;
    public JdbcEntityDefinition(EntityDefinition internal){
        this.internal = internal;
    }

    /**
     *
     * @return internal entity definition
     */
    public EntityDefinition getInternal(){
        return this.internal;
    }

    /**
     * As to GenericMetricEntity, return "${tableName}", else return "${tableName}_${prefix}"
     *
     * @return jdbc table name in lowercase
     */
    public String getJdbcTableName(){
        if(this.internal.getService().equals(GenericMetricEntity.GENERIC_METRIC_SERVICE)){
            return this.internal.getTable().toLowerCase();
        }else {
            return String.format("%s_%s", this.internal.getTable(),this.internal.getPrefix()).toLowerCase();
        }
    }

    public Class<?> getColumnType(String fieldName) throws NoSuchFieldException {
        return internal.getEntityClass().getField(fieldName).getType();
    }

    /**
     *
     * TODO: Generate table schema DDL according entity definition
     *
     * @link https://db.apache.org/ddlutils/
     *
     * CREATE TABLE ${prefix}${tableName}{
     *      prefix prefix;
     *      encodedRowkey varchar;
     *      intField1 int;
     *      longField bitint;
     *      tag varchar;
     * } PRIMARY KEY(encodedRowkey);
     *
     * CREATE TABLE ${metricTable}{
     *      encodedRowkey varchar;
     *      prefix varchar;
     *      intField1 int;
     *      longField bitint;
     *      tag varchar;
     * } PRIMARY KEY(rowkey,prefix);
     *
     * @param tagsFields
     * @return
     */
    @SuppressWarnings("unused")
    public String getJdbcSchemaDDL(String[] tagsFields){
        throw new RuntimeException("TODO: not implemented yet");
    }

    @SuppressWarnings("unchecked")
    public JdbcSerDeser getJdbcSerDeser(String columnName) {
        Qualifier qualifier = this.internal.getQualifierNameMap().get(columnName);
        if(qualifier == null){
            return JdbcEntityDefinitionManager.DEFAULT_JDBC_SERDESER;
        }else {
            return JdbcEntityDefinitionManager.getJdbcSerDeser(qualifier.getSerDeser().getClass());
        }
    }

    public boolean isGenericMetric(){
        return this.internal.getEntityClass().equals(GenericMetricEntity.class);
    }
}