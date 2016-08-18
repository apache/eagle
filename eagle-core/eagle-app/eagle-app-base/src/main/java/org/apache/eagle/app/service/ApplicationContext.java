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
package org.apache.eagle.app.service;

import com.google.common.base.Preconditions;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.eagle.app.Application;
import org.apache.eagle.app.ApplicationLifecycle;
import org.apache.eagle.app.environment.ExecutionRuntime;
import org.apache.eagle.app.environment.ExecutionRuntimeManager;
import org.apache.eagle.metadata.model.ApplicationEntity;
import org.apache.eagle.metadata.model.StreamDesc;
import org.apache.eagle.metadata.model.StreamSinkConfig;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Managed Application Interface: org.apache.eagle.app.service.ApplicationContext
 *
 * <ul>
 *     <li>Application Metadata Entity (Persistence): org.apache.eagle.metadata.model.ApplicationEntity</li>
 *     <li>Application Processing Logic (Execution): org.apache.eagle.app.Application</li>
 *     <li>Application Lifecycle Listener (Installation): org.apache.eagle.app.ApplicationLifecycle</li>
 * </ul>
 */
public class ApplicationContext implements Serializable, ApplicationLifecycle {
    private final Config config;
    private final Application application;
    private final ExecutionRuntime runtime;
    private final ApplicationEntity metadata;

    /**
     * @param metadata ApplicationEntity
     * @param application Application
     */
    public ApplicationContext(Application application, ApplicationEntity metadata, Config envConfig){
        Preconditions.checkNotNull(application,"Application is null");
        Preconditions.checkNotNull(metadata,"ApplicationEntity is null");
        this.application = application;
        this.metadata = metadata;
        this.runtime = ExecutionRuntimeManager.getInstance().getRuntime(application.getEnvironmentType(),envConfig);
        Map<String,Object> executionConfig = metadata.getConfiguration();
        if(executionConfig == null) {
            executionConfig = Collections.emptyMap();
        }

        // TODO: Decouple hardcoded configuration key
        executionConfig.put("siteId", metadata.getSite().getSiteId());
        executionConfig.put("mode", metadata.getMode().name());
        executionConfig.put("appId", metadata.getAppId());
        executionConfig.put("jarPath", metadata.getJarPath());
        this.config = ConfigFactory.parseMap(executionConfig).withFallback(envConfig);
    }

    @Override
    public void onInstall() {
        List<StreamDesc> streamDescCollection = metadata.getDescriptor().getStreams().stream().map((streamDefinition -> {
            StreamSinkConfig streamSinkConfig = this.runtime.environment().streamSink().getSinkConfig(streamDefinition.getStreamId(),this.config);
            StreamDesc streamDesc = new StreamDesc();
            streamDesc.setSchema(streamDefinition);
            streamDesc.setSink(streamSinkConfig);
            streamDesc.setStreamId(streamDefinition.getStreamId());
            return streamDesc;
        })).collect(Collectors.toList());
        metadata.setStreams(streamDescCollection);
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
