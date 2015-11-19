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
package org.apache.eagle.storage.jdbc.entity;

import org.apache.eagle.log.base.taggedlog.TaggedLogAPIEntity;
import org.apache.eagle.log.entity.GenericMetricEntity;
import org.apache.eagle.log.entity.meta.EntityDefinition;
import org.apache.eagle.log.entity.meta.Qualifier;
import org.apache.eagle.storage.jdbc.JdbcConstants;
import org.apache.eagle.storage.jdbc.schema.JdbcEntityDefinition;
import org.apache.eagle.storage.jdbc.schema.JdbcEntityDefinitionManager;
import org.apache.eagle.storage.jdbc.schema.serializer.JdbcSerDeser;
import org.apache.commons.beanutils.PropertyUtils;
import org.apache.torque.ColumnImpl;
import org.apache.torque.util.ColumnValues;
import org.apache.torque.util.JdbcTypedValue;

import java.beans.PropertyDescriptor;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.HashMap;
import java.util.Map;

/**
 * @since 3/26/15
 */
public class JdbcEntitySerDeserHelper {
    /**
     *
     * @param row
     * @param entityDefinition
     * @param <E>
     * @return
     * @throws IllegalAccessException
     * @throws InstantiationException
     * @throws InvocationTargetException
     * @throws NoSuchMethodException
     */
    public static <E extends TaggedLogAPIEntity> E buildEntity(Map<String, Object> row, JdbcEntityDefinition entityDefinition) throws IllegalAccessException, InstantiationException, InvocationTargetException, NoSuchMethodException {
        EntityDefinition ed = entityDefinition.getInternal();

        Class<? extends TaggedLogAPIEntity> clazz = ed.getEntityClass();
        if(clazz == null){
            throw new NullPointerException("Entity class of service "+ed.getService()+" is null");
        }

        TaggedLogAPIEntity obj = clazz.newInstance();
        Map<String, Qualifier> map = ed.getDisplayNameMap();
        for(Map.Entry<String, Object> entry : row.entrySet()){
            // timestamp;
            if(JdbcConstants.TIMESTAMP_COLUMN_NAME.equals(entry.getKey())){
                obj.setTimestamp((Long) entry.getValue());
                continue;
            }

            // set metric as prefix for generic metric
            if(entityDefinition.getInternal().getService().equals(GenericMetricEntity.GENERIC_METRIC_SERVICE) &&
                    JdbcConstants.METRIC_NAME_COLUMN_NAME.equals(entry.getKey())){
                obj.setPrefix((String) entry.getValue());
                continue;
            }

            // rowkey: uuid
            if(JdbcConstants.ROW_KEY_COLUMN_NAME.equals(entry.getKey())){
                obj.setEncodedRowkey((String) entry.getValue());
                continue;
            }

            Qualifier q = map.get(entry.getKey().toLowerCase());
            if(q == null){
                // if it's not pre-defined qualifier, it must be tag unless it's a bug
                if(obj.getTags() == null){
                    obj.setTags(new HashMap<String, String>());
                }
                obj.getTags().put(entry.getKey(), (String) entry.getValue());
                continue;
            }

            // parse different types of qualifiers
            String fieldName = q.getDisplayName();
            // PropertyDescriptor pd = PropertyUtils.getPropertyDescriptor(obj, fieldName);
            PropertyDescriptor pd = getPropertyDescriptor(obj,fieldName);
            if(entry.getValue() != null){
                pd.getWriteMethod().invoke(obj, entry.getValue());
            }
        }

        if(!entityDefinition.getInternal().getService().equals(GenericMetricEntity.GENERIC_METRIC_SERVICE) ){
            obj.setPrefix(entityDefinition.getInternal().getPrefix());
        }
        return (E)obj;
    }

    private final static Map<String,PropertyDescriptor> _propertyDescriptorCache = new HashMap<String, PropertyDescriptor>();

    /**
     *
     * @param obj
     * @param fieldName
     * @return
     * @throws IllegalAccessException
     * @throws NoSuchMethodException
     * @throws InvocationTargetException
     */
    public static PropertyDescriptor getPropertyDescriptor(Object obj,String fieldName) throws IllegalAccessException, NoSuchMethodException, InvocationTargetException {
        String key = obj.getClass().getName()+"."+fieldName;
        PropertyDescriptor propertyDescriptor = _propertyDescriptorCache.get(key);
        if(propertyDescriptor ==null){
            propertyDescriptor = PropertyUtils.getPropertyDescriptor(obj, fieldName);
            _propertyDescriptorCache.put(key,propertyDescriptor);
        }
        return propertyDescriptor;
    }

    /**
     *
     * @param resultSet
     * @param entityDefinition
     * @param <E>
     * @return
     * @throws IOException
     * @throws SQLException
     * @throws InvocationTargetException
     * @throws NoSuchMethodException
     * @throws InstantiationException
     * @throws IllegalAccessException
     */
    public static <E extends TaggedLogAPIEntity> E readEntity(ResultSet resultSet, JdbcEntityDefinition entityDefinition) throws IOException, SQLException, InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException {
        return buildEntity(readInternal(resultSet, entityDefinition), entityDefinition);
    }

    /**
     *
     * @param resultSet
     * @param entityDefinition
     * @return
     * @throws SQLException
     * @throws IOException
     */
    public static Map<String,Object> readInternal(ResultSet resultSet, JdbcEntityDefinition entityDefinition) throws SQLException, IOException {
        ResultSetMetaData metadata = resultSet.getMetaData();
        int columnCount = metadata.getColumnCount();
        Map<String,Object> row = new HashMap<String, Object>();
        for (int i = 1; i <= columnCount; i++) {
            String columnName = metadata.getColumnName(i);
            JdbcSerDeser serDeser = entityDefinition.getJdbcSerDeser(columnName);
            if(serDeser==null){
                throw new IOException("SQLSerDeser for column: "+columnName+" is null");
            }
            Object value = serDeser.readValue(resultSet, columnName, entityDefinition);
            row.put(columnName,value);
        }
        return row;
    }

    /**
     *
     * @param entity
     * @param jdbcEntityDefinition
     * @param <E>
     * @return
     * @throws InvocationTargetException
     * @throws IllegalAccessException
     */
    public static <E extends TaggedLogAPIEntity> ColumnValues buildColumnValues(E entity, JdbcEntityDefinition jdbcEntityDefinition) throws InvocationTargetException, IllegalAccessException {
        String tableName = jdbcEntityDefinition.getJdbcTableName();
        ColumnValues columnValues = new ColumnValues();

        if(jdbcEntityDefinition.isGenericMetric()){
            columnValues.put(new ColumnImpl(tableName, JdbcConstants.METRIC_NAME_COLUMN_NAME),new JdbcTypedValue(entity.getPrefix(), Types.VARCHAR));
        }

        columnValues.put(new ColumnImpl(tableName, JdbcConstants.ROW_KEY_COLUMN_NAME),new JdbcTypedValue(entity.getEncodedRowkey(), Types.VARCHAR));
        columnValues.put(new ColumnImpl(tableName, JdbcConstants.TIMESTAMP_COLUMN_NAME),new JdbcTypedValue(entity.getTimestamp(), Types.BIGINT));

        // qualifier fields
        Map<String,Qualifier> displayQualifier = jdbcEntityDefinition.getInternal().getDisplayNameMap();
        Map<String,Method> getMethods = jdbcEntityDefinition.getInternal().getQualifierGetterMap();
        for(Map.Entry<String,Qualifier> entry:displayQualifier.entrySet()){
            String displayName = entry.getKey();

            Qualifier qualifier = entry.getValue();
//            String qualifierName = qualifier.getQualifierName();
            Method getMethod = getMethods.get(displayName);
            Object fieldValue = getMethod.invoke(entity);

            Class<?> fieldType = qualifier.getSerDeser().type();
            JdbcSerDeser jdbcSerDeser = JdbcEntityDefinitionManager.getJdbcSerDeser(fieldType);
            JdbcTypedValue jdbcTypedValue = jdbcSerDeser.getJdbcTypedValue(fieldValue, fieldType);
            columnValues.put(new ColumnImpl(tableName,displayName),jdbcTypedValue);
        }

        // tag fields
        if(entity.getTags()!=null) {
            for (Map.Entry<String, String> tag : entity.getTags().entrySet()) {
                columnValues.put(new ColumnImpl(tableName, tag.getKey()), new JdbcTypedValue(tag.getValue(), Types.VARCHAR));
            }
        }
        return columnValues;
    }
}