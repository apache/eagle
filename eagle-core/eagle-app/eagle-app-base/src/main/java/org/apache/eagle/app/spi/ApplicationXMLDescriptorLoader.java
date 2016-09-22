/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.eagle.app.spi;

import org.apache.eagle.app.Application;
import org.apache.eagle.app.config.ApplicationProviderDescConfig;
import org.apache.eagle.app.utils.DynamicJarPathFinder;
import org.apache.eagle.metadata.model.ApplicationDesc;

/**
 * Describe Application metadata with XML descriptor configuration in path of:  /META-INF/providers/${ApplicationProviderClassName}.xml
 */
class ApplicationXMLDescriptorLoader implements ApplicationDescLoader {
    private final Class<? extends ApplicationProvider> providerClass;
    private final Class<? extends Application> applicationClass;

    private static final String METADATA_RESOURCE_PATH = "/META-INF/providers/%s.xml";

    ApplicationXMLDescriptorLoader(Class<? extends ApplicationProvider> providerClass, Class<? extends Application> applicationClass) {
        this.providerClass = providerClass;
        this.applicationClass = applicationClass;
    }

    /**
     * Default metadata path is:  /META-INF/providers/${ApplicationProviderClassName}.xml
     * <p>You are not recommended to override this method except you could make sure the path is universal unique</p>
     *
     * @return metadata file path
     */
    private String generateXMLDescriptorPath() {
        return String.format(METADATA_RESOURCE_PATH, providerClass.getName());
    }

    @Override
    public ApplicationDesc getApplicationDesc() {
        String descriptorPath = generateXMLDescriptorPath();
        ApplicationDesc applicationDesc = new ApplicationDesc();
        applicationDesc.setProviderClass(this.providerClass);
        ApplicationProviderDescConfig descWrapperConfig = ApplicationProviderDescConfig.loadFromXML(this.getClass(), descriptorPath);
        applicationDesc.setType(descWrapperConfig.getType());
        applicationDesc.setVersion(descWrapperConfig.getVersion());
        applicationDesc.setName(descWrapperConfig.getName());
        applicationDesc.setDocs(descWrapperConfig.getDocs());
        applicationDesc.setJarPath(DynamicJarPathFinder.findPath(applicationClass));
        if (applicationClass != null) {
            applicationDesc.setAppClass(applicationClass);
            if (!Application.class.isAssignableFrom(applicationDesc.getAppClass())) {
                throw new IllegalStateException(applicationDesc.getAppClass() + " is not sub-class of " + Application.class.getCanonicalName());
            }
        }
        applicationDesc.setDependencies(descWrapperConfig.getDependencies());
        applicationDesc.setViewPath(descWrapperConfig.getViewPath());
        applicationDesc.setConfiguration(descWrapperConfig.getConfiguration());
        applicationDesc.setStreams(descWrapperConfig.getStreams());
        return applicationDesc;
    }
}
