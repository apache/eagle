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
package org.apache.eagle.app.module;

import com.typesafe.config.ConfigFactory;
import org.apache.eagle.app.service.ApplicationHealthCheckService;
import org.apache.eagle.app.service.ApplicationManagementService;
import org.apache.eagle.app.service.ApplicationProviderService;
import org.apache.eagle.app.service.impl.ApplicationHealthCheckServiceImpl;
import org.apache.eagle.app.service.impl.ApplicationManagementServiceImpl;
import org.apache.eagle.app.service.impl.ApplicationProviderServiceImpl;
import org.apache.eagle.app.service.impl.ApplicationStatusUpdateServiceImpl;
import org.apache.eagle.metadata.service.ApplicationDescService;
import com.google.inject.AbstractModule;
import com.google.inject.Singleton;
import com.google.inject.util.Providers;
import org.apache.eagle.metadata.service.ApplicationStatusUpdateService;

public class ApplicationGuiceModule extends AbstractModule {
    private final ApplicationProviderService appProviderInst;

    public ApplicationGuiceModule(ApplicationProviderService appProviderInst) {
        this.appProviderInst = appProviderInst;
    }

    public ApplicationGuiceModule() {
        this.appProviderInst = new ApplicationProviderServiceImpl(ConfigFactory.load());
    }

    @Override
    protected void configure() {
        bind(ApplicationProviderService.class).toProvider(Providers.of(appProviderInst));
        bind(ApplicationDescService.class).toProvider(Providers.of(appProviderInst));
        bind(ApplicationManagementService.class).to(ApplicationManagementServiceImpl.class).in(Singleton.class);
        bind(ApplicationStatusUpdateService.class).to(ApplicationStatusUpdateServiceImpl.class).in(Singleton.class);
        bind(ApplicationHealthCheckService.class).to(ApplicationHealthCheckServiceImpl.class).in(Singleton.class);
    }
}