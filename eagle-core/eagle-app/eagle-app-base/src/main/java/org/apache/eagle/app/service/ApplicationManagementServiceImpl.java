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
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.typesafe.config.Config;
import org.apache.eagle.app.Application;
import org.apache.eagle.app.ApplicationContext;
import org.apache.eagle.metadata.model.ApplicationDesc;
import org.apache.eagle.metadata.model.ApplicationEntity;
import org.apache.eagle.metadata.model.SiteEntity;
import org.apache.eagle.metadata.service.ApplicationEntityService;
import org.apache.eagle.metadata.service.SiteEntityService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Singleton
public class ApplicationManagementServiceImpl implements ApplicationManagementService {
    private final SiteEntityService siteEntityService;
    private final ApplicationProviderService applicationProviderService;
    private final ApplicationEntityService applicationEntityService;
    private final Config config;
    private final static Logger LOGGER = LoggerFactory.getLogger(ApplicationManagementServiceImpl.class);

    @Inject
    public ApplicationManagementServiceImpl(
            Config config,
            SiteEntityService siteEntityService,
            ApplicationProviderService applicationProviderService,
            ApplicationEntityService applicationEntityService){
        this.config = config;
        this.siteEntityService = siteEntityService;
        this.applicationProviderService = applicationProviderService;
        this.applicationEntityService = applicationEntityService;
    }

    public ApplicationEntity install(AppOperations.InstallOperation operation) {
        Preconditions.checkNotNull(operation.getSiteId(),"siteId is null");
        Preconditions.checkNotNull(operation.getAppType(),"appType is null");
        SiteEntity siteEntity = siteEntityService.getBySiteId(operation.getSiteId());
        Preconditions.checkNotNull(siteEntity,"Site with ID: "+operation.getSiteId()+" is not found");
        ApplicationDesc appDesc = applicationProviderService.getApplicationDescByType(operation.getAppType());
        Preconditions.checkNotNull("Application with TYPE: "+operation.getAppType()+" is not found");
        ApplicationEntity applicationEntity = new ApplicationEntity();
        applicationEntity.setDescriptor(appDesc);
        applicationEntity.setSite(siteEntity);
        applicationEntity.setConfiguration(operation.getConfiguration());
        applicationEntity.setMode(operation.getMode());
        ApplicationContext applicationContext = new ApplicationContext(applicationEntity,config);
        applicationEntity.setStreams(applicationContext.getStreamSinkDescs());
        applicationEntityService.create(applicationEntity);
        return applicationEntity;
    }

    public ApplicationEntity uninstall(AppOperations.UninstallOperation operation) {
        ApplicationEntity applicationEntity = applicationEntityService.getByUUIDOrAppId(operation.getUuid(),operation.getAppId());
        Application application = applicationProviderService.getApplicationProviderByType(applicationEntity.getDescriptor().getType()).getApplication();
        // TODO: Check status, skip stop if already STOPPED
        try {
            application.stop(new ApplicationContext(applicationEntity, this.config));
        }catch (Throwable throwable){
            LOGGER.error(throwable.getMessage(),throwable);
        }
        return applicationEntityService.delete(applicationEntity);
    }

    public ApplicationEntity start(AppOperations.StartOperation operation) {
        ApplicationEntity applicationEntity = applicationEntityService.getByUUIDOrAppId(operation.getUuid(),operation.getAppId());
        Application application = applicationProviderService.getApplicationProviderByType(applicationEntity.getDescriptor().getType()).getApplication();
        application.start(new ApplicationContext(applicationEntity,this.config));
        return applicationEntity;
    }

    public ApplicationEntity stop(AppOperations.StopOperation operation) {
        ApplicationEntity applicationEntity = applicationEntityService.getByUUIDOrAppId(operation.getUuid(),operation.getAppId());
        Application application = applicationProviderService.getApplicationProviderByType(applicationEntity.getDescriptor().getType()).getApplication();
        application.stop(new ApplicationContext(applicationEntity,this.config));
        return applicationEntity;
    }
}