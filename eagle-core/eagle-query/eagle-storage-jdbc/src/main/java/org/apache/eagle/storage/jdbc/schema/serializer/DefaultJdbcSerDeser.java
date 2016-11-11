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
package org.apache.eagle.storage.jdbc.schema.serializer;

import org.apache.eagle.log.entity.meta.Qualifier;
import org.apache.eagle.storage.jdbc.JdbcConstants;
import org.apache.eagle.storage.jdbc.schema.JdbcEntityDefinitionManager;
import org.apache.torque.util.JdbcTypedValue;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;

public class DefaultJdbcSerDeser<T, R> implements JdbcSerDeser<T> {

    @Override
    public T toJavaTypedValue(ResultSet result, Class<?> fieldType, String fieldName, Qualifier qualifier) throws IOException {
        int jdbcType = JdbcEntityDefinitionManager.getJdbcType(fieldType);
        try {
            if (Types.JAVA_OBJECT == jdbcType) {
                byte[] bytes = result.getBytes(fieldName);
                return (T) qualifier.getSerDeser().deserialize(bytes);
            } else if (Types.BOOLEAN == jdbcType) {
                return (T) new Boolean(result.getBoolean(fieldName));
            } else {
                return (T) result.getObject(fieldName);
            }
        } catch (SQLException e) {
            throw new IOException("Field: " + fieldName + ", java type:" + fieldType + ", jdbc type: " + jdbcType, e);
        }
    }

    @Override
    public JdbcTypedValue toJdbcTypedValue(Object fieldValue, Class<?> fieldType, Qualifier qualifier) {
        int jdbcTypeCode = JdbcEntityDefinitionManager.getJdbcType(fieldType);
        if (Types.JAVA_OBJECT == jdbcTypeCode) {
            byte[] bytes = qualifier.getSerDeser().serialize(fieldValue);
            return new JdbcTypedValue(bytes, JdbcConstants.DEFAULT_TYPE_FOR_COMPLEX_TYPE);
        } else {
            return new JdbcTypedValue(fieldValue, jdbcTypeCode);
        }
    }
}