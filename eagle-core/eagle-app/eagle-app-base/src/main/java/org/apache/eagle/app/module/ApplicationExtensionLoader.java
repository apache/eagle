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

import org.apache.eagle.app.service.ApplicationProviderService;
import org.apache.eagle.common.module.ModuleRegistry;
import org.apache.eagle.common.module.ModuleRegistryImpl;
import com.google.inject.Guice;
import com.google.inject.Module;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ApplicationExtensionLoader {
    private static final Logger LOGGER = LoggerFactory.getLogger(ApplicationExtensionLoader.class);

    public static ModuleRegistry load(Module... context) {
        LOGGER.warn("Loading application extension modules");
        ModuleRegistry registry = new ModuleRegistryImpl();
        Guice.createInjector(context).getInstance(ApplicationProviderService.class).getProviders().forEach((provider) -> {
            LOGGER.warn("Registering modules from {}", provider);
            provider.register(registry);
        });
        return registry;
    }
}
