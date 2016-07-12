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
package org.apache.eagle.app.base.persistence;

import com.typesafe.config.Config;
import net.sf.extcos.ComponentQuery;
import net.sf.extcos.ComponentScanner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public abstract class AbstractPersistence implements Persistence {
    private final static String PACKAGE_SCOPE = "org.apache.eagle";
    private final Map<Class<?>,Object> realizationTypeInstanceMap = new HashMap<>();
    private final Config config;

    public AbstractPersistence(Config config){
        this.config = config;
        load();
    }

    @Override
    public <T> void register(T realization) {
        Class<T> realizationType = (Class<T>) realization.getClass();
        if(realizationTypeInstanceMap.containsKey(realizationType)){
            throw new IllegalStateException("Duplicated realization found of realization type: "+realizationType.getCanonicalName()+" in persistence: "+this.getClass().getCanonicalName());
        }
        LOG.info("Registered {}",realizationType.getCanonicalName());
        realizationTypeInstanceMap.put(realizationType,realization);
    }

    @Override
    public <T> T realize(Class<T> type) {
        if(!type.isInterface()) throw new IllegalArgumentException(type+" is not interface");

        List<T> result = new ArrayList<>();
        realizationTypeInstanceMap.entrySet().stream().filter(entry -> type.isAssignableFrom(entry.getKey())).forEach(entry -> {
            result.add((T) entry.getValue());
        });
        if(result.size() == 0){
            return null;
        } else if(result.size()>1){
            throw new IllegalStateException("Too many ("+result.size()+") realizations found for "+type);
        }
        return result.get(0);
    }

    private void load() {
        LOG.info("Loading repositories for persistence [{}] under package [{}]",this.getClass().getCanonicalName(),PACKAGE_SCOPE);
        final ComponentScanner scanner = new ComponentScanner();
        final Set<Class<?>> classes = scanner.getClasses(new RepositoryScanQuery());
        int count = 0;
        for (Class<?> realizationClass : classes) {
            org.apache.eagle.app.base.persistence.annotation.Persistence persistence = realizationClass.getDeclaredAnnotation(org.apache.eagle.app.base.persistence.annotation.Persistence.class);
            if(persistence == null) throw new RuntimeException("@Persistence is not found on repository class:  "+realizationClass.getCanonicalName());
            if(persistence.value() == null) throw new RuntimeException("@Persistence value is null on repository class: "+realizationClass.getCanonicalName());
            if(persistence.value() == getClass()) {
                Object repository;
                try {
                    repository = realizationClass.newInstance();
                    register(repository);
                } catch (InstantiationException | IllegalAccessException e) {
                    throw new RuntimeException(e.getMessage(), e.getCause());
                }
                count ++;
            } else {
                 LOG.debug("Skipped {} ({})",realizationClass.getCanonicalName(),persistence.value().getSimpleName());
            }
        }
        LOG.info("Loaded {} realizations (totally {}) for persistence: {}",count,classes.size(),this.getClass().getCanonicalName());
    }

    private static final Logger LOG = LoggerFactory.getLogger(AbstractPersistence.class);
    public static class RepositoryScanQuery extends ComponentQuery {
        @Override
        protected void query() {
//            select().from(PACKAGE_SCOPE).returning(allImplementing(Repository.class));
            select().from(PACKAGE_SCOPE).returning(allAnnotatedWith(org.apache.eagle.app.base.persistence.annotation.Persistence.class));
        }
    }
}