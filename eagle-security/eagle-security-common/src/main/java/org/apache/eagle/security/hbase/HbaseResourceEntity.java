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

package org.apache.eagle.security.hbase;

import com.google.common.base.Objects;

import java.io.Serializable;
import java.util.Set;

public class HbaseResourceEntity implements Serializable {
    private String resource;
    private String namespace;
    private String table;
    private String columnFamily;
    private String sensitiveType;
    private Set<String> childSensitiveTypes;



    public HbaseResourceEntity(String resource, String ns, String table, String cf, String sensitiveType, Set<String> childSensitiveTypes) {
        this.resource = resource;
        this.namespace = ns;
        this.table = table;
        this.columnFamily = cf;
        this.sensitiveType = sensitiveType;
        this.childSensitiveTypes = childSensitiveTypes;
    }

    public String getResource() {
        return resource;
    }

    public void setResource(String resource) {
        this.resource = resource;
    }

    public String getNamespace() {
        return namespace;
    }

    public void setNamespace(String namespace) {
        this.namespace = namespace;
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public String getColumnFamily() {
        return columnFamily;
    }

    public void setColumnFamily(String columnFamily) {
        this.columnFamily = columnFamily;
    }

    public String getSensitiveType() {
        return sensitiveType;
    }

    public void setSensitiveType(String sensitiveType) {
        this.sensitiveType = sensitiveType;
    }

    public Set<String> getChildSensitiveTypes() {
        return childSensitiveTypes;
    }

    public void setChildSensitiveTypes(Set<String> childSensitiveTypes) {
        this.childSensitiveTypes = childSensitiveTypes;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        final HbaseResourceEntity other = (HbaseResourceEntity) obj;
        return Objects.equal(this.resource, other.resource)
                && this.namespace.equals(other.namespace)
                && this.table.equals(other.table)
                && this.columnFamily.equals(other.columnFamily)
                && this.sensitiveType.equals(other.sensitiveType)
                && this.childSensitiveTypes.containsAll(other.childSensitiveTypes);
    }
}
