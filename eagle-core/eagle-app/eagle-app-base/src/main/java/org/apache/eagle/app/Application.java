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
package org.apache.eagle.app;

import org.apache.eagle.app.environment.Environment;

import java.io.Serializable;
import java.util.Map;

/**
 * Application Execution Interface
 *
 * <h1>Design Principle</h1>
 * <ul>
 *  <li>Easy to develop and extend </li>
 *  <li>Easy to test and run locally</li>
 *  <li>Easy to manage lifecycle through framework</li>
 * </ul>
 *
 * @param <Proc>
 * @param <Conf>
 * @param <Env>
 */
public interface Application <
    Conf extends Configuration,     //  Application Configuration
    Env extends Environment,        // Application Environment
    Proc                            // Application Process
> extends Serializable {
    /**
     * Execute with type-safe configuration
     *
     * Developer-oriented interface
     *
     * @param config application configuration
     * @param environment execution environment
     * @return execution process
     */
    Proc execute(Conf config, Env environment);

    /**
     * Execute with raw map-based configuration
     *
     * Management service oriented interface
     *
     * @param config application configuration
     * @param environment  execution environment
     * @return execution process
     */
    Proc execute(Map<String,Object> config, Env environment);

    /**
     * Execute with environment based configuration
     *
     * Light-weight Runner (dry-run/test purpose) oriented interface
     *
     * @param environment  execution environment
     * @return execution process
     */
    Proc execute(Env environment);

    /**
     * @return application configuration type (POJO class)
     */
    Class<Conf> getConfigType();

    /**
     * @return application environment type
     */
    Class<? extends Env> getEnvironmentType();
}