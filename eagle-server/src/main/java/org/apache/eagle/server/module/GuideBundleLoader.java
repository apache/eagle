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
package org.apache.eagle.server.module;

import com.google.inject.Module;
import com.hubspot.dropwizard.guice.GuiceBundle;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.eagle.app.module.ApplicationExtensionLoader;
import org.apache.eagle.app.service.ApplicationProviderService;
import org.apache.eagle.app.service.impl.ApplicationProviderServiceImpl;
import org.apache.eagle.common.module.GlobalScope;
import org.apache.eagle.common.module.ModuleRegistry;
import org.apache.eagle.metadata.persistence.MetadataStore;
import org.apache.eagle.metadata.persistence.MetadataStoreModuleFactory;
import org.apache.eagle.server.ServerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class GuideBundleLoader {
    private final static Logger LOGGER = LoggerFactory.getLogger(GuideBundleLoader.class);

    public static GuiceBundle<ServerConfig> load(List<Module> modules){
        /*
           We use tow injectors, one is Dropwizard injector, the other injector is to instantiate ApplicationProvider and
           load sub modules from applications
           so we need make Config and ApplicationProviderServiceImpl to have a global instance across multiple injectors
         */

        // Eagle server module
        Config config = ConfigFactory.load();
        ApplicationProviderService appProviderSvc = new ApplicationProviderServiceImpl(config);
        ServerModule serveBaseModule = new ServerModule(appProviderSvc);

        // load application specific modules
        ModuleRegistry registry = ApplicationExtensionLoader.load(serveBaseModule);

        // add application specific modules
        MetadataStore metadataStoreModule = MetadataStoreModuleFactory.getModule();
        List<Module> metadataExtensions = metadataStoreModule.getModules(registry);
        int extensionNum = 0;
        GuiceBundle.Builder<ServerConfig> builder = GuiceBundle.newBuilder();
        if(metadataExtensions!=null){
            extensionNum = metadataExtensions.size();
            metadataExtensions.forEach(builder::addModule);
        }
        LOGGER.warn("Loaded {} modules (scope: metadataStore)",extensionNum);

        List<Module> globalExtensions = registry.getModules(GlobalScope.class);
        extensionNum = 0;
        if(globalExtensions!=null){
            extensionNum = globalExtensions.size();
            globalExtensions.forEach(builder::addModule);
        }
        LOGGER.warn("Loaded {} modules (scope: global)",extensionNum);

        if(modules!=null) modules.forEach(builder::addModule);
        return builder.addModule(serveBaseModule)
                .setConfigClass(ServerConfig.class)
                .build();
    }

    public static GuiceBundle<ServerConfig> load(){
        return load(null);
    }
}