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
package org.apache.eagle.security.hive.entity;

import com.google.common.base.Objects;

import java.io.Serializable;
import java.util.Set;

public class HiveResourceEntity implements Serializable {
    private String resource;
    private String database;
    private String table;
    private String column;
    private String sensitiveType;
    private Set<String> childSensitiveTypes;


    public Set<String> getChildSensitiveTypes() {
        return childSensitiveTypes;
    }

    public void setChildSensitiveTypes(Set<String> childSensitiveTypes) {
        this.childSensitiveTypes = childSensitiveTypes;
    }

    public String getResource() {
        return resource;
    }

    public void setResource(String resource) {
        this.resource = resource;
    }

    public String getDatabase() {
        return database;
    }

    public void setDatabase(String database) {
        this.database = database;
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public String getColumn() {
        return column;
    }

    public void setColumn(String column) {
        this.column = column;
    }

    public String getSensitiveType() {
        return sensitiveType;
    }

    public void setSensitiveType(String sensitiveType) {
        this.sensitiveType = sensitiveType;
    }


    public HiveResourceEntity(String resource, String database, String table, String column, String sensitiveType, Set<String> childSensitiveTypes) {
        this.resource = resource;
        this.database = database;
        this.table = table;
        this.column = column;
        this.sensitiveType = sensitiveType;
        this.childSensitiveTypes = childSensitiveTypes;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        final HiveResourceEntity other = (HiveResourceEntity) obj;
        return Objects.equal(this.resource, other.resource)
                && this.database.equals(other.database)
                && this.table.equals(other.table)
                && this.column.equals(other.column)
                && this.sensitiveType.equals(other.sensitiveType)
                && this.childSensitiveTypes.containsAll(other.childSensitiveTypes);
    }
}
