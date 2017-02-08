/*
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
package org.apache.eagle.app.config;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

class ApplicationConfigMeta<T extends Object> {
    private final Class<? extends T> configurationClass;
    private final Map<Field, Config> fieldProperties;
    private final Map<String, Field> fieldNameFields;


    ApplicationConfigMeta(Class<? extends T> configurationClass) {
        this.configurationClass = configurationClass;
        fieldNameFields = new HashMap<>();
        fieldProperties = new HashMap<>();
        load();
    }

    private void load() {
        Field[] fields = this.configurationClass.getDeclaredFields();
        for (Field field : fields) {
            fieldNameFields.put(field.getName(), field);
            if (field.isAnnotationPresent(Config.class)) {
                Config config = field.getAnnotation(Config.class);
                fieldProperties.put(field, config);
            }
        }
    }

    public boolean hasAnyDeclaredProperties() {
        return fieldProperties.size() > 0;
    }

    public Class<? extends T> getConfigClass() {
        return configurationClass;
    }

    public Set<String> getFieldNames() {
        return fieldNameFields.keySet();
    }

    public Field getField(String field) {
        return fieldNameFields.get(field);
    }

    public Map<Field, Config> getFieldProperties() {
        return fieldProperties;
    }

    public T newInstance() throws IllegalAccessException, InstantiationException {
        return configurationClass.newInstance();
    }
}