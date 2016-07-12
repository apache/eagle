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
import com.typesafe.config.ConfigFactory;
import org.apache.eagle.app.base.persistence.memory.Memory;

import java.lang.reflect.InvocationTargetException;

public class PersistenceManager {
    private Persistence persistence;
    private void load() {
        Config config = ConfigFactory.load();
        Class<? extends Persistence> persistenceAdapter = null;
        try {
            if (config.hasPath("persistence.type")) {
                persistenceAdapter = (Class<? extends Persistence>) Class.forName(config.getString("persistence.type"));
            } else {
                persistenceAdapter = Memory.class;
            }
            persistence = persistenceAdapter.getConstructor(Config.class).newInstance(config);
        } catch ( InstantiationException
                | IllegalAccessException
                | ClassNotFoundException
                | NoSuchMethodException
                | InvocationTargetException e) {
            throw new RuntimeException("Failed to initialize "+persistenceAdapter,e);
        }
    }

    public <T> T realize(Class<T> repositoryClass){
        return persistence.realize(repositoryClass);
    }

    private static PersistenceManager instance = null;

    public static PersistenceManager getInstance(){
        if(instance == null){
            instance = new PersistenceManager();
            instance.load();
        }
        return instance;
    }
}