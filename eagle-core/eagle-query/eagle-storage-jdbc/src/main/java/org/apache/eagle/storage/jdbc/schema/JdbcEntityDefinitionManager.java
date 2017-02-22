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

import org.apache.eagle.log.base.taggedlog.TaggedLogAPIEntity;
import org.apache.eagle.log.entity.meta.EntityDefinition;
import org.apache.eagle.log.entity.meta.EntityDefinitionManager;
import org.apache.eagle.log.entity.meta.EntitySerDeser;
import org.apache.eagle.storage.jdbc.schema.serializer.JdbcSerDeser;
import org.apache.eagle.storage.jdbc.schema.serializer.DefaultJdbcSerDeser;
import org.apache.eagle.storage.jdbc.schema.serializer.MetricJdbcSerDeser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Types;
import java.util.HashMap;
import java.util.Map;

/**
 *
 * Manage the basic repository of all entity definitions and JDBC specific metadata
 *
 * @since 3/27/15
 */
public class JdbcEntityDefinitionManager {
    private final static Logger LOG = LoggerFactory.getLogger(JdbcEntityDefinitionManager.class);
    private final static Map<Class<? extends TaggedLogAPIEntity>,JdbcEntityDefinition> sqlEntityDefinitionCache = new HashMap<Class<? extends TaggedLogAPIEntity>,JdbcEntityDefinition>();
    private static Boolean initialized = false;

    public static JdbcEntityDefinition getJdbcEntityDefinition(EntityDefinition entityDefinition){
        checkInit();

        Class<? extends TaggedLogAPIEntity> entityClass = entityDefinition.getEntityClass();
        JdbcEntityDefinition jdbcEntityDefinition = sqlEntityDefinitionCache.get(entityClass);
        if(jdbcEntityDefinition == null){
            jdbcEntityDefinition = new JdbcEntityDefinition(entityDefinition);
            sqlEntityDefinitionCache.put(entityClass, jdbcEntityDefinition);
        }
        return jdbcEntityDefinition;
    }

    public static Map<Class<? extends TaggedLogAPIEntity>,JdbcEntityDefinition> getJdbcEntityDefinitionMap(){
        checkInit();
        return sqlEntityDefinitionCache;
    }

    public static JdbcEntityDefinition getJdbcEntityDefinition(Class<? extends TaggedLogAPIEntity> clazz) throws IllegalAccessException, InstantiationException {
        checkInit();
        return getJdbcEntityDefinition(EntityDefinitionManager.getEntityDefinitionByEntityClass(clazz));
    }

    private static void checkInit(){
        if (!initialized) {
            try {
                Map<String,EntityDefinition> entries = EntityDefinitionManager.entities();
                for (Map.Entry<String, EntityDefinition> entry : entries.entrySet()) {
                    Class<? extends TaggedLogAPIEntity> entityClass = entry.getValue().getEntityClass();
                    JdbcEntityDefinition jdbcEntityDefinition = sqlEntityDefinitionCache.get(entityClass);
                    if(jdbcEntityDefinition == null){
                        jdbcEntityDefinition = new JdbcEntityDefinition(entry.getValue());
                        sqlEntityDefinitionCache.put(entityClass, jdbcEntityDefinition);
                    }
                }
                initialized = true;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    public static void load(){
        checkInit();
    }

    public static JdbcSerDeser DEFAULT_JDBC_SERDESER = new DefaultJdbcSerDeser();
    public static JdbcSerDeser METRIC_JDBC_SERDESER = new MetricJdbcSerDeser();
    private final static Map<Class<?>,JdbcSerDeser> _columnTypeSerDeserMapping = new HashMap<Class<?>, JdbcSerDeser>();

    /**
     *
     * @param serDeser
     */
    public static void registerJdbcSerDeser(Class<? extends EntitySerDeser> entitySerDeser,JdbcSerDeser serDeser){
        if(entitySerDeser == null || serDeser == null){
            throw new IllegalArgumentException("should not be null");
        }
        _columnTypeSerDeserMapping.put(entitySerDeser, serDeser);
    }

    @SuppressWarnings("unchecked")
    public static <T> JdbcSerDeser<T> getJdbcSerDeser(Class<?> type){
        JdbcSerDeser serDeser = _columnTypeSerDeserMapping.get(type);
        if(serDeser == null) {
            return DEFAULT_JDBC_SERDESER;
        }else{
            return serDeser;
        }
    }


    private final static Map<Class<?>,Integer> _classJdbcType = new HashMap<Class<?>, Integer>();

    /**
     * Get corresponding SQL types for certain entity field class type
     *
     * @see java.sql.Types
     *
     * @param fieldType entity field type class
     * @return java.sql.Types, return Types.NULL if not found
     */
    public static Integer getJdbcType(Class<?> fieldType) {
        if(fieldType == null){
            return Types.NULL;
        } else if(!_classJdbcType.containsKey(fieldType)){
            if(LOG.isDebugEnabled())
                LOG.debug("Unable to locate simple jdbc type for: {}, return type as JAVA_OBJECT",fieldType);
            return Types.JAVA_OBJECT;
        }
        return _classJdbcType.get(fieldType);
    }


    /**
     * Register fieldType with SQL types
     *
     * @see java.sql.Types
     *
     * @param fieldType entity field type class
     * @param jdbcType java.sql.Types
     */
    public static void registerJdbcType(Class<?> fieldType, Integer jdbcType){
        _classJdbcType.put(fieldType,jdbcType);
    }

    //================================================
    // Initially bind basic java types with SQL types
    //================================================
    static {
        registerJdbcType(String.class, Types.VARCHAR);
        registerJdbcType(Integer.class, Types.INTEGER);
        registerJdbcType(Double.class, Types.DOUBLE);
        registerJdbcType(Float.class, Types.FLOAT);
        registerJdbcType(Long.class, Types.BIGINT);

        registerJdbcType(int.class, Types.INTEGER);
        registerJdbcType(float.class, Types.FLOAT);
        registerJdbcType(double.class, Types.DOUBLE);
        registerJdbcType(long.class, Types.BIGINT);
        registerJdbcType(short.class, Types.INTEGER);
       //  registerJdbcType(char[].class, Types.VARCHAR);
        registerJdbcType(char.class, Types.CHAR);

        registerJdbcType(Boolean.class, Types.BOOLEAN);
        registerJdbcType(boolean.class, Types.BOOLEAN);
    }
}