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
package org.apache.eagle.app.service.impl;

import com.typesafe.config.Config;
import org.apache.eagle.app.config.ApplicationProviderConfig;
import org.apache.eagle.app.service.ApplicationProviderLoader;
import org.apache.eagle.app.spi.ApplicationProvider;
import org.apache.eagle.app.tools.DynamicJarPathFinder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ServiceLoader;
import java.util.function.Function;

public class ApplicationProviderSPILoader extends ApplicationProviderLoader{
    private final String appProviderExtDir;
    private final static Logger LOG = LoggerFactory.getLogger(ApplicationProviderSPILoader.class);
    private final static String APPLICATIONS_DIR_PROPS_KEY = "application.provider.dir";

    public ApplicationProviderSPILoader(Config config) {
        super(config);
        if(config.hasPath(APPLICATIONS_DIR_PROPS_KEY)) {
            this.appProviderExtDir = config.getString(APPLICATIONS_DIR_PROPS_KEY);
        }else{
            this.appProviderExtDir = null;
        }

        LOG.info("Using {}: {}",APPLICATIONS_DIR_PROPS_KEY,this.appProviderExtDir);
    }

    @Override
    public void load() {
        if(appProviderExtDir != null) {
            LOG.info("Loading application providers from class loader of jars in {}", appProviderExtDir);
            File loc = new File(appProviderExtDir);
            File[] jarFiles = loc.listFiles(file -> file.getPath().toLowerCase().endsWith(".jar"));
            if (jarFiles != null) {
                for (File jarFile : jarFiles) {
                    try {
                        URL jarFileUrl = jarFile.toURI().toURL();
                        LOG.debug("Loading ApplicationProvider from jar: {}", jarFileUrl.toString());
                        URLClassLoader jarFileClassLoader = new URLClassLoader(new URL[]{jarFileUrl});
                        loadProviderFromClassLoader(jarFileClassLoader, (applicationProviderConfig) -> jarFileUrl.getPath());
                    } catch (Exception e) {
                        LOG.warn("Failed to load application provider from jar {}", jarFile,e);
                    }
                }
            }
        } else {
            LOG.info("Loading application providers from context class loader");
            ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
            loadProviderFromClassLoader(classLoader,(applicationProviderConfig) -> DynamicJarPathFinder.findPath(applicationProviderConfig.getClass()));
        }
    }

    private void loadProviderFromClassLoader(ClassLoader jarFileClassLoader, Function<ApplicationProviderConfig,String> jarFileSupplier){
        ServiceLoader<ApplicationProvider> serviceLoader = ServiceLoader.load(ApplicationProvider.class, jarFileClassLoader);
        for (ApplicationProvider applicationProvider : serviceLoader) {
            try {
                ApplicationProviderConfig providerConfig = new ApplicationProviderConfig();
                providerConfig.setClassName(applicationProvider.getClass().getCanonicalName());
                providerConfig.setJarPath(jarFileSupplier.apply(providerConfig));
                applicationProvider.prepare(providerConfig, getConfig());
                registerProvider(applicationProvider);
            }catch (Throwable ex){
                LOG.warn("Failed to register application provider {}",applicationProvider,ex);
            }
        }
    }
}
