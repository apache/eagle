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
package org.apache.eagle.app.environment;

import org.apache.eagle.app.environment.impl.*;
import com.google.common.base.Preconditions;
import com.typesafe.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;

/**
 * Manage execution runtime corresponding to Environment.
 *
 * @see Environment
 * @see ExecutionRuntime
 */
public class ExecutionRuntimeManager {
    private static final ExecutionRuntimeManager INSTANCE = new ExecutionRuntimeManager();
    private static final Logger LOGGER = LoggerFactory.getLogger(ExecutionRuntimeManager.class);

    static {
        getInstance().register(StormEnvironment.class, new StormExecutionRuntime.Provider());
        getInstance().register(SparkEnvironment.class, new SparkExecutionRuntime.Provider());
        getInstance().register(WebEnvironment.class, new WebExecutionRuntime.Provider());
    }

    private final Map<Class<? extends Environment>, ExecutionRuntimeProvider> executionRuntimeProviders;
    private final Map<Environment, ExecutionRuntime> executionRuntimeCache;

    private ExecutionRuntimeManager() {
        executionRuntimeProviders = new HashMap<>();
        executionRuntimeCache = new HashMap<>();
    }

    public static ExecutionRuntimeManager getInstance() {
        return INSTANCE;
    }

    public <E extends Environment, P> ExecutionRuntime getRuntime(E environment) {
        Preconditions.checkNotNull(environment, "Failed to create execution runtime as environment is null");
        if (executionRuntimeCache.containsKey(environment)) {
            return executionRuntimeCache.get(environment);
        }

        if (executionRuntimeProviders.containsKey(environment.getClass())) {
            ExecutionRuntime<E, P> runtime = ((ExecutionRuntimeProvider<E, P>) executionRuntimeProviders.get(environment.getClass())).get();
            runtime.prepare(environment);
            executionRuntimeCache.put(environment, runtime);
            LOGGER.info("Created new execution runtime {} for environment: {}", runtime, environment);
            return runtime;
        } else {
            LOGGER.error("No matched execution runtime found for environment: " + environment);
            throw new IllegalStateException("No matched execution runtime found for environment: " + environment);
        }
    }

    public <E extends Environment> ExecutionRuntime getRuntime(Class<E> environmentClass, Config config) {
        try {
            E environment = environmentClass.getConstructor(Config.class).newInstance(config);
            return getRuntime(environment);
        } catch (InstantiationException | InvocationTargetException | NoSuchMethodException | IllegalAccessException e) {
            LOGGER.error("Failed to create environment instance of type: " + environmentClass, e);
            throw new RuntimeException("Failed to create environment instance of type: " + environmentClass, e);
        }
    }

    public void register(Class<? extends Environment> appSuperClass, ExecutionRuntimeProvider executionRuntimeProvider) {
        if (executionRuntimeProviders.containsKey(appSuperClass)) {
            throw new IllegalStateException("Duplicated application type registered: " + appSuperClass.getCanonicalName());
        }
        executionRuntimeProviders.put(appSuperClass, executionRuntimeProvider);
    }
}