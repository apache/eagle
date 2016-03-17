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
import org.apache.torque.util.JdbcTypedValue;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;

/**
 * @since 3/26/15
 */
public class JsonJdbcSerDeser<T extends Object> implements JdbcSerDeser<T> {
    private static ObjectMapper objectMapper = new ObjectMapper();

    @SuppressWarnings("unchecked")
    @Override
    public T toJavaTypedValue(ResultSet result, Class<?> fieldType, String fieldName, Qualifier qualifier) throws IOException {
        try {
            String jsonString = result.getString(fieldName);
            return (T) objectMapper.readValue(jsonString, fieldType);
        } catch (IOException e) {
           throw e;
        } catch (SQLException e) {
            throw new IOException(e);
        }
    }

    @Override
    public JdbcTypedValue toJdbcTypedValue(Object fieldValue, Class<?> fieldType, Qualifier qualifier) {
        try {
            return new JdbcTypedValue(objectMapper.writeValueAsString(objectMapper.writeValueAsString(fieldValue)), Types.VARCHAR);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }
}
