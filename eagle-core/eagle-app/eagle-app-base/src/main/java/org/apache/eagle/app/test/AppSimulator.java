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

import org.apache.eagle.app.spi.ApplicationProvider;

import java.util.Map;

/**
 * Application test simulator for developer to quickly run application without diving into application lifecycle
 */
public interface AppSimulator {
    /**
     *
     * @param appType
     */
    void submit(String appType);
    /**
     *
     * @param appType
     * @param appConfig
     */
    void submit(String appType, Map<String,Object> appConfig);

    /**
     *
     * @param appProviderClass
     */
    void submit(Class<? extends ApplicationProvider> appProviderClass);

    /**
     *
     * @param appProviderClass
     * @param appConfig
     */
    void submit(Class<? extends ApplicationProvider> appProviderClass, Map<String,Object> appConfig) throws Exception;
}