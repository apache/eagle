package org.apache.eagle.storage.jdbc.schema.serializer;

import org.apache.eagle.log.entity.meta.Qualifier;
import org.apache.eagle.storage.jdbc.JdbcConstants;
import org.apache.eagle.storage.jdbc.schema.JdbcEntityDefinitionManager;
import org.apache.torque.util.JdbcTypedValue;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;

/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
public class MetricJdbcSerDeser implements JdbcSerDeser<double[]> {
    @Override
    public double[] toJavaTypedValue(ResultSet result, Class<?> fieldType, String fieldName, Qualifier qualifier) throws IOException {
        try {
            return new double[]{result.getDouble(fieldName)};
        } catch (SQLException e) {
            throw new IOException("Generic Metric Field: "+fieldName+", java type:"+fieldType,e);
        }
    }

    /**
     *
     * @param fieldValue
     * @param fieldType
     * @return
     */
    @Override
    public JdbcTypedValue toJdbcTypedValue(Object fieldValue, Class<?> fieldType, Qualifier qualifier) {
        double[] metricFieldValues = (double[]) fieldValue;
        if(metricFieldValues == null || metricFieldValues.length == 0){
            return null;
        } else if(metricFieldValues.length > 1){
            throw new IllegalArgumentException("Not support metric value length > 1: "+fieldType);
        }else{
            return new JdbcTypedValue(metricFieldValues[0],Types.DOUBLE);
        }
    }
}
