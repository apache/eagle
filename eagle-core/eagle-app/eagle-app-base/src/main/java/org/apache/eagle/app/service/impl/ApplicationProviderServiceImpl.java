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

import org.apache.eagle.app.service.ApplicationProviderLoader;
import org.apache.eagle.app.service.ApplicationProviderService;
import org.apache.eagle.app.spi.ApplicationProvider;
import org.apache.eagle.metadata.model.ApplicationDesc;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.typesafe.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.stream.Collectors;

/**
 * Support to load application provider from application.provider.config = "providers.xml" configuration file
 * or application.provider.dir = "lib/apps" with SPI Class loader
 * <p>TODO: hot-manage application provider loading</p>
 */
@Singleton
public class ApplicationProviderServiceImpl implements ApplicationProviderService {
    private final Config config;
    private static final Logger LOG = LoggerFactory.getLogger(ApplicationProviderServiceImpl.class);
    private final ApplicationProviderLoader appProviderLoader;
    public static final String APP_PROVIDER_LOADER_CLASS_KEY = "application.provider.loader";

    @Inject
    public ApplicationProviderServiceImpl(Config config) {
        LOG.warn("Initializing {}", this.getClass().getCanonicalName());
        this.config = config;
        String appProviderLoaderClass = this.config.hasPath(APP_PROVIDER_LOADER_CLASS_KEY)
            ? this.config.getString(APP_PROVIDER_LOADER_CLASS_KEY) : ApplicationProviderLoader.getDefaultAppProviderLoader();
        LOG.warn("Initializing {} = {}", APP_PROVIDER_LOADER_CLASS_KEY, appProviderLoaderClass);
        appProviderLoader = initializeAppProviderLoader(appProviderLoaderClass);
        LOG.warn("Initialized {}", appProviderLoader);
        reload();
    }

    private ApplicationProviderLoader initializeAppProviderLoader(String appProviderLoaderClass) {
        try {
            return (ApplicationProviderLoader) Class.forName(appProviderLoaderClass).getConstructor(Config.class).newInstance(this.config);
        } catch (Throwable e) {
            LOG.error("Failed to initialize ApplicationProviderLoader: " + appProviderLoaderClass, e);
            throw new IllegalStateException("Failed to initialize ApplicationProviderLoader: " + appProviderLoaderClass, e);
        }
    }

    public synchronized void reload() {
        appProviderLoader.reset();
        LOG.warn("Loading application providers ...");
        appProviderLoader.load();
        LOG.warn("Loaded {} application providers", appProviderLoader.getProviders().size());
    }

    public Collection<ApplicationProvider> getProviders() {
        return appProviderLoader.getProviders();
    }

    public Collection<ApplicationDesc> getApplicationDescs() {
        return getProviders().stream().map(ApplicationProvider::getApplicationDesc).collect(Collectors.toList());
    }

    public ApplicationProvider<?> getApplicationProviderByType(String type) {
        return appProviderLoader.getApplicationProviderByType(type);
    }

    @Deprecated
    public ApplicationDesc getApplicationDescByType(String appType) {
        return appProviderLoader.getApplicationProviderByType(appType).getApplicationDesc();
    }
}
