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
package org.apache.eagle.app.service.impl;

import org.apache.eagle.alert.metadata.IMetadataDao;
import org.apache.eagle.app.service.ApplicationContext;
import org.apache.eagle.app.service.ApplicationManagementService;
import org.apache.eagle.app.service.ApplicationOperations;
import org.apache.eagle.app.service.ApplicationProviderService;
import org.apache.eagle.app.spi.ApplicationProvider;
import org.apache.eagle.metadata.exceptions.EntityNotFoundException;
import org.apache.eagle.metadata.model.ApplicationDesc;
import org.apache.eagle.metadata.model.ApplicationEntity;
import org.apache.eagle.metadata.model.Property;
import org.apache.eagle.metadata.model.SiteEntity;
import org.apache.eagle.metadata.service.ApplicationEntityService;
import org.apache.eagle.metadata.service.SiteEntityService;
import com.google.common.base.Preconditions;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.typesafe.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Singleton
public class ApplicationManagementServiceImpl implements ApplicationManagementService {
    private final SiteEntityService siteEntityService;
    private final ApplicationProviderService applicationProviderService;
    private final ApplicationEntityService applicationEntityService;
    private final IMetadataDao alertMetadataService;
    private final Config config;
    private static final Logger LOGGER = LoggerFactory.getLogger(ApplicationManagementServiceImpl.class);

    @Inject
    public ApplicationManagementServiceImpl(
        Config config,
        SiteEntityService siteEntityService,
        ApplicationProviderService applicationProviderService,
        ApplicationEntityService applicationEntityService,
        IMetadataDao alertMetadataService) {
        this.config = config;
        this.siteEntityService = siteEntityService;
        this.applicationProviderService = applicationProviderService;
        this.applicationEntityService = applicationEntityService;
        this.alertMetadataService = alertMetadataService;
    }

    @Override
    public ApplicationEntity install(ApplicationOperations.InstallOperation operation) throws EntityNotFoundException {
        Preconditions.checkNotNull(operation.getSiteId(), "siteId is null");
        Preconditions.checkNotNull(operation.getAppType(), "appType is null");
        if (operation.getMode().equals(ApplicationEntity.Mode.CLUSTER)) {
            Preconditions.checkNotNull(operation.getJarPath(), "jarPath is null when mode is CLUSTER");
        }
        SiteEntity siteEntity = siteEntityService.getBySiteId(operation.getSiteId());
        Preconditions.checkNotNull(siteEntity, "Site with ID: " + operation.getSiteId() + " is not found");
        ApplicationDesc appDesc = applicationProviderService.getApplicationDescByType(operation.getAppType());
        Preconditions.checkNotNull("Application with TYPE: " + operation.getAppType() + " is not found");
        ApplicationEntity applicationEntity = new ApplicationEntity();
        applicationEntity.setDescriptor(appDesc);
        applicationEntity.setSite(siteEntity);
        applicationEntity.setMode(operation.getMode());
        applicationEntity.setJarPath(operation.getJarPath());
        applicationEntity.ensureDefault();

        /**
         *  calculate application config based on
         *   1) default values in metadata.xml
         *   2) user's config value override default configurations
         *   3) some metadata, for example siteId, mode, appId in ApplicationContext
         */
        Map<String, Object> appConfig = new HashMap<>();
        ApplicationProvider provider = applicationProviderService.getApplicationProviderByType(operation.getAppType());

        List<Property> propertyList = provider.getApplicationDesc().getConfiguration().getProperties();
        for (Property p : propertyList) {
            appConfig.put(p.getName(), p.getValue());
        }
        if (operation.getConfiguration() != null) {
            appConfig.putAll(operation.getConfiguration());
        }
        applicationEntity.setConfiguration(appConfig);
        ApplicationContext applicationContext = new ApplicationContext(
            applicationProviderService.getApplicationProviderByType(applicationEntity.getDescriptor().getType()).getApplication(),
            applicationEntity, config, alertMetadataService);
        applicationContext.onInstall();
        return applicationEntityService.create(applicationEntity);
    }

    @Override
    public ApplicationEntity uninstall(ApplicationOperations.UninstallOperation operation) {
        ApplicationEntity applicationEntity = applicationEntityService.getByUUIDOrAppId(operation.getUuid(), operation.getAppId());
        ApplicationContext applicationContext = new ApplicationContext(
            applicationProviderService.getApplicationProviderByType(applicationEntity.getDescriptor().getType()).getApplication(),
            applicationEntity, config, alertMetadataService);
        // TODO: Check status, skip stop if already STOPPED
        try {
            applicationContext.onStop();
        } catch (Throwable throwable) {
            LOGGER.error(throwable.getMessage(), throwable);
        }
        applicationContext.onUninstall();
        return applicationEntityService.delete(applicationEntity);
    }

    @Override
    public ApplicationEntity start(ApplicationOperations.StartOperation operation) {
        ApplicationEntity applicationEntity = applicationEntityService.getByUUIDOrAppId(operation.getUuid(), operation.getAppId());
        ApplicationContext applicationContext = new ApplicationContext(
            applicationProviderService.getApplicationProviderByType(applicationEntity.getDescriptor().getType()).getApplication(),
            applicationEntity, config, alertMetadataService);
        applicationContext.onStart();
        return applicationEntity;
    }

    @Override
    public ApplicationEntity stop(ApplicationOperations.StopOperation operation) {
        ApplicationEntity applicationEntity = applicationEntityService.getByUUIDOrAppId(operation.getUuid(), operation.getAppId());
        ApplicationContext applicationContext = new ApplicationContext(
            applicationProviderService.getApplicationProviderByType(applicationEntity.getDescriptor().getType()).getApplication(),
            applicationEntity, config, alertMetadataService);
        applicationContext.onStop();
        return applicationEntity;
    }
}
