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

import com.typesafe.config.Config;
import org.apache.eagle.alert.engine.coordinator.StreamDefinition;
import org.apache.eagle.app.Application;
import org.apache.eagle.app.config.ApplicationProviderConfig;
import org.apache.eagle.app.config.ApplicationProviderDescConfig;
import org.apache.eagle.common.module.ModuleRegistry;
import org.apache.eagle.metadata.model.ApplicationDesc;
import org.apache.eagle.metadata.model.ApplicationDocs;
import org.apache.eagle.metadata.model.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.bind.JAXBException;
import java.util.List;

/**
 * Default metadata path is:  /META-INF/providers/${ApplicationProviderClassName}.xml
 *
 * @param <T>
 */
public abstract class AbstractApplicationProvider<T extends Application> implements ApplicationProvider<T> {
    private final static Logger LOG = LoggerFactory.getLogger(AbstractApplicationProvider.class);
    private final ApplicationDesc applicationDesc;

    private final static String METADATA_RESOURCE_PATH="/META-INF/providers/%s.xml";

    /**
     * Default metadata path is:  /META-INF/providers/${ApplicationProviderClassName}.xml
     *
     * You are not recommended to override this method except you could make sure the path is universal unique
     *
     * @return metadata file path
     */
    protected final String getMetadata(){
        return String.format(METADATA_RESOURCE_PATH,this.getClass().getName());
    }

    protected AbstractApplicationProvider() {
        String applicationDescConfig = getMetadata();
        applicationDesc = new ApplicationDesc();
        applicationDesc.setProviderClass(this.getClass());
        ApplicationProviderDescConfig descWrapperConfig = ApplicationProviderDescConfig.loadFromXML(this.getClass(), applicationDescConfig);
        setType(descWrapperConfig.getType());
        setVersion(descWrapperConfig.getVersion());
        setName(descWrapperConfig.getName());
        setDocs(descWrapperConfig.getDocs());
        try {
            if (descWrapperConfig.getAppClass() != null) {
//                setAppClass((Class<T>) Class.forName(descWrapperConfig.getAppClass()));
                setAppClass((Class<T>) Class.forName(descWrapperConfig.getAppClass(), true, this.getClass().getClassLoader()));
                if (!Application.class.isAssignableFrom(applicationDesc.getAppClass())) {
                    throw new IllegalStateException(descWrapperConfig.getAppClass() + " is not sub-class of " + Application.class.getCanonicalName());
                }
            }
        } catch (ClassNotFoundException e) {
            LOG.error(e.getMessage(), e);
            throw new RuntimeException(e.getMessage(), e.getCause());
        }
        setViewPath(descWrapperConfig.getViewPath());
        setConfiguration(descWrapperConfig.getConfiguration());
        setStreams(descWrapperConfig.getStreams());
    }

    @Override
    public void prepare(ApplicationProviderConfig providerConfig, Config envConfig) {
        this.applicationDesc.setJarPath(providerConfig.getJarPath());
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
            LOG.error("Failed to load configuration template from "+resourceName,e);
            throw new RuntimeException("Failed to load configuration template from "+resourceName,e);
        }
    }

    @Override
    public String toString() {
        return String.format(
                "%s[name=%s, type=%s, version=%s, viewPath=%s, appClass=%s, configuration= %s properties]", getClass().getSimpleName(),
                applicationDesc.getName(),applicationDesc.getType(),applicationDesc.getVersion(), applicationDesc.getViewPath(), applicationDesc.getAppClass(), applicationDesc.getConfiguration().size()
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

    @Override
    public void register(ModuleRegistry registry) {
        LOG.debug("Registering modules {}",this.getClass().getName());
    }
}