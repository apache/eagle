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
package org.apache.eagle.app.test;

import com.google.inject.Guice;
import com.google.inject.Module;
import org.apache.eagle.app.spi.ApplicationProvider;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Application test simulator for developer to quickly run application without diving into application lifecycle
 */
public abstract class ServerSimulator {
    /**
     *
     * @param appType
     */
    public abstract void start(String appType);

    /**
     *
     * @param appType
     * @param appConfig
     */
    public abstract void start(String appType, Map<String,Object> appConfig);

    /**
     *
     * @param appProviderClass
     */
    public abstract void start(Class<? extends ApplicationProvider> appProviderClass);

    /**
     *
     * @param appProviderClass
     * @param appConfig
     */
    public abstract void start(Class<? extends ApplicationProvider> appProviderClass, Map<String,Object> appConfig) throws Exception;

    public static ServerSimulator getInstance(){
        return Guice.createInjector(new AppTestGuiceModule()).getInstance(ServerSimulator.class);
    }

    /**
     * @param modules additional modules
     * @return ServerSimulator instance
     */
    public static ServerSimulator getInstance(Module ... modules){
        List<Module> contextModules = Arrays.asList(modules);
        contextModules.add(new AppTestGuiceModule());
        return Guice.createInjector(contextModules).getInstance(ServerSimulator.class);
    }
}