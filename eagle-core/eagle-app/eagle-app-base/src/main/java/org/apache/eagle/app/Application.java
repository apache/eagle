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
import com.typesafe.config.Config;

import java.io.Serializable;

/**
 * Application Execution Interface.
 * <h1>Design Principle</h1>
 * <ul>
 * <li>Easy to develop and extend </li>
 * <li>Easy to test and run locally</li>
 * <li>Easy to manage lifecycle through framework</li>
 * </ul>
 *
 * @param <P>
 * @param <E>
 */
public interface Application<
    E extends Environment,        // Application Environment
    P                            // Application Process
    > extends Serializable {
    /**
     * Execute with application configuration.
     *
     * @param config      application configuration
     * @param environment execution environment
     * @return execution process
     */
    P execute(Config config, E environment);

    /**
     * Execution Environment type.
     *
     * @return application environment type
     */
    Class<? extends E> getEnvironmentType();
}