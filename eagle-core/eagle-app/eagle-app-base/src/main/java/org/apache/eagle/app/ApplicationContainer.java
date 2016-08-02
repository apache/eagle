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
package org.apache.eagle.app;

import com.typesafe.config.Config;
import org.apache.eagle.app.environment.ExecutionRuntime;
import org.apache.eagle.app.environment.ExecutionRuntimeManager;
import org.apache.eagle.app.utils.ApplicationConfigHelper;
import org.apache.eagle.metadata.model.ApplicationEntity;

import java.io.Serializable;
import java.util.Map;

/**
 * Managed Application Interface: org.apache.eagle.app.ApplicationContainer
 *
 * <ul>
 *     <li>Application Metadata Entity (Persistence): org.apache.eagle.metadata.model.ApplicationEntity</li>
 *     <li>Application Processing Logic (Execution): org.apache.eagle.app.Application</li>
 *     <li>Application Lifecycle Listener (Installation): org.apache.eagle.app.ApplicationLifecycle</li>
 * </ul>
 */
public class ApplicationContainer implements Serializable, ApplicationLifecycle {
    private final ApplicationConfig config;
    private final Application application;
    private final ExecutionRuntime runtime;
    private final ApplicationEntity metadata;

    /**
     * @param metadata ApplicationEntity
     * @param application Application
     */
    public ApplicationContainer(Application application, ApplicationEntity metadata, Config config){
        this.application = application;
        this.metadata = metadata;
        this.runtime = ExecutionRuntimeManager.getInstance().getRuntime(application.getEnvironmentClass(),config);
        Map<String,Object> applicationConfig = metadata.getConfiguration();
        this.config = ApplicationConfigHelper.convertFrom(applicationConfig,application.getConfigClass());
        this.config.setMode(metadata.getMode());
        this.config.setAppId(metadata.getAppId());
    }

    @Override
    public void onInstall() {
        //
    }

    @Override
    public void onUninstall() {
        //
    }

    @Override
    public void onStart() {
        this.runtime.start(this.application,this.config);
    }

    @Override
    public void onStop() {
        this.runtime.stop(this.application,this.config);
    }

    public ApplicationEntity getMetadata() {
        return metadata;
    }
}