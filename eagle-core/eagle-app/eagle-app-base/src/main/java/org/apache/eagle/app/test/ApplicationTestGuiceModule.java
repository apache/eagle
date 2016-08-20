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

import com.google.inject.AbstractModule;
import com.google.inject.Singleton;
import com.typesafe.config.ConfigFactory;
import org.apache.eagle.app.module.ApplicationExtensionLoader;
import org.apache.eagle.app.module.ApplicationGuiceModule;
import org.apache.eagle.app.service.ApplicationProviderService;
import org.apache.eagle.app.service.impl.ApplicationProviderServiceImpl;
import org.apache.eagle.app.spi.ApplicationProvider;
import org.apache.eagle.common.module.CommonGuiceModule;
import org.apache.eagle.common.module.GlobalScope;
import org.apache.eagle.common.module.ModuleRegistry;
import org.apache.eagle.metadata.service.memory.MemoryMetadataStore;

public class ApplicationTestGuiceModule extends AbstractModule{
    @Override
    protected void configure() {
        CommonGuiceModule common = new CommonGuiceModule();
        ApplicationProviderService instance = new ApplicationProviderServiceImpl(ConfigFactory.load());
        ApplicationGuiceModule app = new ApplicationGuiceModule(instance);
        MemoryMetadataStore store = new MemoryMetadataStore();
        install(common);
        install(app);
        install(store);
        ModuleRegistry registry =ApplicationExtensionLoader.load(common,app,store);
        registry.getModules(store.getClass()).forEach(this::install);
        registry.getModules(GlobalScope.class).forEach(this::install);
        bind(ApplicationSimulator.class).to(ApplicationSimulatorImpl.class).in(Singleton.class);
    }
}