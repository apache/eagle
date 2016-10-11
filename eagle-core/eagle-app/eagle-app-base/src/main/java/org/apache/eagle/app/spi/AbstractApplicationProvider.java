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

package org.apache.eagle.app.spi;

import com.google.common.base.Preconditions;
import com.google.inject.AbstractModule;
import org.apache.eagle.alert.engine.coordinator.StreamDefinition;
import org.apache.eagle.app.Application;
import org.apache.eagle.app.service.ApplicationListener;
import org.apache.eagle.common.module.GlobalScope;
import org.apache.eagle.common.module.ModuleRegistry;
import org.apache.eagle.common.module.ModuleScope;
import org.apache.eagle.metadata.model.ApplicationDesc;
import org.apache.eagle.metadata.model.ApplicationDocs;
import org.apache.eagle.metadata.model.ApplicationEntity;
import org.apache.eagle.metadata.model.Configuration;
import org.apache.eagle.metadata.persistence.MetadataStore;
import org.apache.eagle.metadata.service.memory.MemoryMetadataStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.bind.JAXBException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * Describe Application metadata with XML descriptor configuration in path of:  /META-INF/providers/${ApplicationProviderClassName}.xml.
 */
public abstract class AbstractApplicationProvider<T extends Application> implements ApplicationProvider<T> {
    private static final Logger LOG = LoggerFactory.getLogger(AbstractApplicationProvider.class);
    private final ApplicationDesc applicationDesc;

    protected AbstractApplicationProvider() {
        applicationDesc = new ApplicationXMLDescriptorLoader(this.getClass(),this.getApplicationClass()).getApplicationDesc();
    }

    protected void setVersion(String version) {
        applicationDesc.setVersion(version);
    }

    protected void setName(String name) {
        applicationDesc.setName(name);
    }

    protected void setAppClass(Class<T> appClass) {
        applicationDesc.setAppClass(appClass);
    }

    protected void setViewPath(String viewPath) {
        applicationDesc.setViewPath(viewPath);
    }

    protected void setConfiguration(Configuration configuration) {
        applicationDesc.setConfiguration(configuration);
    }

    protected void setAppConfig(String resourceName) {
        try {
            applicationDesc.setConfiguration(Configuration.fromResource(resourceName));
        } catch (JAXBException e) {
            LOG.error("Failed to load configuration template from " + resourceName, e);
            throw new RuntimeException("Failed to load configuration template from " + resourceName, e);
        }
    }

    @Override
    public String toString() {
        return String.format(
            "%s[name=%s, type=%s, version=%s, viewPath=%s, appClass=%s, configuration= %s properties]", getClass().getSimpleName(),
            applicationDesc.getName(), applicationDesc.getType(), applicationDesc.getVersion(), applicationDesc.getViewPath(), applicationDesc.getAppClass(),
            applicationDesc.getConfiguration() == null ? 0 : applicationDesc.getConfiguration().size()
        );
    }

    protected void setStreams(List<StreamDefinition> streams) {
        applicationDesc.setStreams(streams);
    }


    protected void setDocs(ApplicationDocs docs) {
        applicationDesc.setDocs(docs);
    }

    public void setType(String type) {
        applicationDesc.setType(type);
    }

    @Override
    public ApplicationDesc getApplicationDesc() {
        return applicationDesc;
    }

    private ModuleRegistry currentRegistry;

    @Override
    public final void register(ModuleRegistry registry) {
        LOG.debug("Registering modules {}", this.getClass().getName());
        this.currentRegistry = registry;
        onRegister();
    }

    @Override
    public Optional<ApplicationListener> getApplicationListener(ApplicationEntity applicationEntity) {
        return Optional.empty();
    }

    protected void onRegister() {
        // Do nothing by default;
    }

    protected  <M extends ModuleScope,T> void bind(Class<M> scope, Class<T> type, Class<? extends T> impl) {
        Preconditions.checkNotNull(currentRegistry, "No registry set before being used");
        currentRegistry.register(scope, new AbstractModule() {
            @Override
            protected void configure() {
                bind(type).to(impl);
            }
        });
    }

    public <T> void bind(Class<T> type, Class<? extends T> impl) {
        bind(GlobalScope.class,type,impl);
    }

    protected <M extends MetadataStore,T> void bindToMetaStore(Class<? extends M> scope, Class<T> type, Class<? extends T> impl) {
        bind(scope,type,impl);
    }

    public <T> void bindToMemoryMetaStore(Class<T> type, Class<? extends T> impl) {
        bindToMetaStore(MemoryMetadataStore.class,type,impl);
    }
}