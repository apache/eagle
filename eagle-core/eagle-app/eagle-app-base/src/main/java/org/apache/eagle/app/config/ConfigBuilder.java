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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.typesafe.config.Config;
import org.apache.commons.beanutils.PropertyUtils;
import org.apache.commons.lang3.StringUtils;

import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;

public class ConfigBuilder<T extends Object> {
    private final ApplicationConfigMeta<T> meta;

    private ConfigBuilder(Class<T> configurationClass) {
        this.meta = new ApplicationConfigMeta<>(configurationClass);
    }

    private void mapTo(Config config, Object configuration) {
        meta.getFieldProperties().forEach((field, property) -> {
            String path = StringUtils.isEmpty(property.value()) ? property.name() : property.value();
            if (StringUtils.isEmpty(path)) {
                path = field.getName();
            }
            Preconditions.checkNotNull(path);
            if (config.hasPath(path) || property.required()) {
                try {
                    Object fieldVal = config.getAnyRef(path);

                    // Handle POJO type config field
                    if (!Map.class.isAssignableFrom(field.getType()) && fieldVal.getClass().equals(HashMap.class)) {
                        fieldVal = new ObjectMapper().convertValue(fieldVal, field.getType());
                    }
                    PropertyUtils.setProperty(configuration, field.getName(), fieldVal);
                } catch (IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
                    throw new IllegalArgumentException(e.getMessage(), e);
                }
            }
        });
    }

    public T mapFrom(Config config) {
        if (!this.meta.hasAnyDeclaredProperties()) {
            return bindWith(config);
        }
        try {
            T configuration = this.meta.newInstance();
            mapTo(config, configuration);
            return configuration;
        } catch (InstantiationException | IllegalAccessException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    public T bindWith(Config config) {
        return new ObjectMapper().convertValue(config.root().unwrapped(), this.meta.getConfigClass());
    }

    private static final Map<Class<?>, ConfigBuilder<?>> CACHE = new HashMap<>();

    public static <T> ConfigBuilder<T> typeOf(Class<T> clazz) {
        synchronized (ConfigBuilder.class) {
            if (!CACHE.containsKey(clazz)) {
                ConfigBuilder<T> mapper = new ConfigBuilder<>(clazz);
                CACHE.put(clazz, mapper);
                return mapper;
            } else {
                return (ConfigBuilder<T>) CACHE.get(clazz);
            }
        }
    }
}
