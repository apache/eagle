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

import com.google.common.base.Preconditions;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.typesafe.config.Config;
import org.apache.eagle.app.service.ApplicationProviderLoader;
import org.apache.eagle.app.service.ApplicationProviderService;
import org.apache.eagle.app.spi.ApplicationProvider;
import org.apache.eagle.metadata.model.ApplicationDependency;
import org.apache.eagle.metadata.model.ApplicationDesc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
    private static final String APP_PROVIDER_LOADER_CLASS_KEY = "application.provider.loader";

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
        validate();
    }

    private void validate() {
        final Map<String, ApplicationDesc> viewPathAppDesc = new HashMap<>();

        for (ApplicationDesc applicationDesc : getApplicationDescs()) {
            LOG.debug("Validating {}", applicationDesc.getType());

            Preconditions.checkNotNull(applicationDesc.getType(), "type is null in " + applicationDesc);
            Preconditions.checkNotNull(applicationDesc.getVersion(), "version is null in " + applicationDesc);
            Preconditions.checkNotNull(applicationDesc.getName(), "name is null in " + applicationDesc);

            if (applicationDesc.getViewPath() != null) {
                if (viewPathAppDesc.containsKey(applicationDesc.getViewPath())) {
                    throw new IllegalStateException("Duplicated view " + applicationDesc.getViewPath()
                        + " defined in " + viewPathAppDesc.get(applicationDesc.getViewPath()).getType() + " and " + applicationDesc.getType());
                } else {
                    viewPathAppDesc.put(applicationDesc.getViewPath(), applicationDesc);
                }
            }

            // Validate Dependency
            LOG.debug("Validating dependency of {}", applicationDesc.getType());
            List<ApplicationDependency> dependencyList = applicationDesc.getDependencies();
            if (dependencyList != null) {
                for (ApplicationDependency dependency : dependencyList) {
                    try {
                        ApplicationDesc dependencyDesc = getApplicationDescByType(dependency.getType());
                        if (dependencyDesc != null && dependency.getVersion() != null) {
                            if (dependencyDesc.getVersion().equals(dependency.getVersion())) {
                                LOG.debug("Loaded dependency {} -> {}", applicationDesc.getType(), dependency);
                            } else {
                                LOG.warn("Loaded dependency {} -> {}, but the version was mismatched, expected: {}, actual: {}",
                                    applicationDesc.getType(), dependency, dependency.getVersion(), applicationDesc.getVersion());
                            }
                        } else {
                            assert dependencyDesc != null;
                            dependency.setVersion(dependencyDesc.getVersion());
                        }
                    } catch (IllegalArgumentException ex) {
                        if (!dependency.isRequired()) {
                            LOG.warn("Unable to load dependency {} -> {}", applicationDesc.getType(), dependency, ex);
                        } else {
                            LOG.error("Failed to load dependency {} -> {}", applicationDesc.getType(), dependency, ex);
                            throw new IllegalStateException("Failed to load application providers due to dependency missing " + applicationDesc.getType() + " -> " + dependency, ex);
                        }
                    }
                }
            }
            LOG.info("Validated {} successfully", applicationDesc.getType());
        }
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

    public ApplicationDesc getApplicationDescByType(String appType) {
        return appProviderLoader.getApplicationProviderByType(appType).getApplicationDesc();
    }
}
