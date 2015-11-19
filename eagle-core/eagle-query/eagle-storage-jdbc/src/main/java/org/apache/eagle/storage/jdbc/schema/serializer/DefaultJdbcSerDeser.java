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

import org.apache.eagle.storage.jdbc.schema.JdbcEntityDefinition;
import org.apache.eagle.storage.jdbc.schema.JdbcEntityDefinitionManager;
import org.apache.torque.util.JdbcTypedValue;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * @since 3/26/15
 */
public class DefaultJdbcSerDeser<T,R> implements JdbcSerDeser<T> {

    @Override
    public T readValue(ResultSet result, String fieldName, JdbcEntityDefinition JdbcEntityDefinition) throws IOException {
        try {
            Object val = result.getObject(fieldName);
            return (T) val;
        } catch (SQLException e) {
            throw new IOException(e);
        }
    }

    /**
     *
     * @param fieldValue
     * @param fieldType
     * @return
     */
    @Override
    public JdbcTypedValue getJdbcTypedValue(Object fieldValue, Class<?> fieldType) {
        return new JdbcTypedValue(fieldValue, JdbcEntityDefinitionManager.getJdbcType(fieldType));
    }
}