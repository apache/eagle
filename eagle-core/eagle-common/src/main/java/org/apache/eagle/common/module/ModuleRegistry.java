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
package org.apache.eagle.common.module;

import com.google.inject.Module;

import java.util.List;

/**
 * Module Registry Repository
 */
public interface ModuleRegistry {
    /**
     * @param modules
     */
    void register(Class<? extends ModuleScope> scope, Module ... modules);

    /**
     *
     * @param moduleScope
     * @return
     */
    List<Module> getModules(Class<? extends ModuleScope> moduleScope);

    /**
     * @return
     */
    List<Class<? extends ModuleScope>> getScopes();

    /**
     * @return all modules
     */
    List<Module> getModules();

    default void register(Module ... modules){
        register(GlobalScope.class,modules);
    }
}