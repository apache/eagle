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

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import junit.framework.Assert;
import org.apache.commons.collections.map.HashedMap;
import org.apache.eagle.alert.metadata.IMetadataDao;
import org.apache.eagle.alert.metadata.impl.InMemMetadataDaoImpl;
import org.apache.eagle.app.service.impl.ApplicationHealthCheckServiceImpl;
import org.apache.eagle.app.service.impl.ApplicationManagementServiceImpl;
import org.apache.eagle.app.service.impl.ApplicationProviderServiceImpl;
import org.apache.eagle.metadata.exceptions.ApplicationWrongStatusException;
import org.apache.eagle.metadata.exceptions.EntityNotFoundException;
import org.apache.eagle.metadata.model.ApplicationDependency;
import org.apache.eagle.metadata.model.ApplicationDesc;
import org.apache.eagle.metadata.model.ApplicationEntity;
import org.apache.eagle.metadata.model.SiteEntity;
import org.apache.eagle.metadata.service.ApplicationEntityService;
import org.apache.eagle.metadata.service.SiteEntityService;
import org.apache.eagle.metadata.service.memory.ApplicationEntityServiceMemoryImpl;
import org.apache.eagle.metadata.service.memory.SiteEntityEntityServiceMemoryImpl;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import static org.mockito.Mockito.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * @Since 11/25/16.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({ApplicationManagementServiceImpl.class})
public class TestApplicationManagementServiceImpl {

    SiteEntityService siteEntityService;
    ApplicationProviderService applicationProviderService;
    ApplicationEntityService applicationEntityService;
    Config config;
    IMetadataDao alertMetadataService;
    ApplicationHealthCheckService applicationHealthCheckService;
    ApplicationManagementService applicationManagementService;
    ApplicationOperations.InstallOperation installOperation;
    ApplicationOperations.UninstallOperation uninstallOperation;
    ApplicationOperations.StartOperation startOperation;
    ApplicationOperations.StopOperation stopOperation;
    ApplicationOperations.CheckStatusOperation checkStatusOperation;
    ApplicationEntity resultEntity;

    @Before
    public void setUp() throws EntityNotFoundException{
        siteEntityService = mock(SiteEntityEntityServiceMemoryImpl.class);
        applicationEntityService = mock(ApplicationEntityServiceMemoryImpl.class);
        config = ConfigFactory.parseMap(new HashMap<String, String>() {
            {
                put("application.healthCheck.initialDelay", "10000");
                put("application.healthCheck.period", "10000");
                put("application.healthCheck.publisher.publisherImpl", "org.apache.eagle.app.service.impl.ApplicationHealthCheckEmailPublisher");
                put("application.healthCheck.publisher.dailySendHour", "11");
                put("service.timezone", "UTC");
                put("application.provider.loader", "org.apache.eagle.app.service.impl.ApplicationProviderSPILoader");
                put("dataSinkConfig.topic", "test_topic");
                put("dataSinkConfig.brokerList", "sandbox.hortonworks.com:6667");
                put("spoutNum", "4");
                put("appId", "testAppId");
            }
        });
        alertMetadataService = new InMemMetadataDaoImpl(config);
        applicationProviderService = new ApplicationProviderServiceImpl(config);
        applicationHealthCheckService = mock(ApplicationHealthCheckServiceImpl.class);
        applicationManagementService = new ApplicationManagementServiceImpl(config, siteEntityService, applicationProviderService, applicationEntityService,
            alertMetadataService, applicationHealthCheckService);
        installOperation = new ApplicationOperations.InstallOperation("sandbox", "TEST_APPLICATION", ApplicationEntity.Mode.LOCAL, "/user");
        startOperation = new ApplicationOperations.StartOperation(null, "TEST_APPLICATION_SANDBOX");
        stopOperation = new ApplicationOperations.StopOperation(null, "TEST_APPLICATION_SANDBOX");
        uninstallOperation = new ApplicationOperations.UninstallOperation(null, "TEST_APPLICATION_SANDBOX");
        checkStatusOperation = new ApplicationOperations.CheckStatusOperation(null, "TEST_APPLICATION_SANDBOX");
        when(siteEntityService.getBySiteId(anyString())).thenReturn(generateSiteEntity());
    }


    @Test
    public void testInstall() throws EntityNotFoundException {
        when(applicationEntityService.create(any(ApplicationEntity.class))).thenReturn(generateCommonEntity());
        resultEntity = applicationManagementService.install(installOperation);
        Assert.assertNotNull(resultEntity);
    }

    @Test
    public void testUnInstall() throws ApplicationWrongStatusException {
        when(applicationEntityService.getByUUIDOrAppId(anyString(), anyString())).thenReturn(generateCommonEntity());
        resultEntity = applicationManagementService.uninstall(uninstallOperation);
        verify(applicationEntityService, times(1)).delete(any(ApplicationEntity.class));
    }

    @Test
    public void testStart() throws Exception {
        when(applicationEntityService.getByUUIDOrAppId(anyString(), anyString())).thenReturn(generateCommonEntity());
        ApplicationAction applicationAction = mock(ApplicationAction.class);
        PowerMockito.whenNew(ApplicationAction.class).withArguments(anyObject(), anyObject(), anyObject(), anyObject()).thenReturn(applicationAction);
        resultEntity = applicationManagementService.start(startOperation);
        verify(applicationAction, times(1)).doStart();
        verify(applicationEntityService, times(1)).create(any(ApplicationEntity.class));
    }

    @Test
    public void testStop() throws Exception {
        when(applicationEntityService.getByUUIDOrAppId(anyString(), anyString())).thenReturn(generateRunningEntity());
        ApplicationAction applicationAction = mock(ApplicationAction.class);
        PowerMockito.whenNew(ApplicationAction.class).withArguments(anyObject(), anyObject(), anyObject(), anyObject()).thenReturn(applicationAction);
        resultEntity = applicationManagementService.stop(stopOperation);
        verify(applicationAction, times(1)).doStop();
        verify(applicationEntityService, times(1)).delete(any(ApplicationEntity.class));
        verify(applicationEntityService, times(1)).create(any(ApplicationEntity.class));
    }

    @Test
    public void testGetStatus() throws Exception {
        when(applicationEntityService.getByUUIDOrAppId(anyString(), anyString())).thenReturn(generateRunningEntity());
        ApplicationAction applicationAction = mock(ApplicationAction.class);
        PowerMockito.whenNew(ApplicationAction.class).withArguments(anyObject(), anyObject(), anyObject(), anyObject()).thenReturn(applicationAction);
        when(applicationAction.getStatus()).thenReturn(ApplicationEntity.Status.RUNNING);
        ApplicationEntity.Status status = applicationManagementService.getStatus(checkStatusOperation);
        Assert.assertEquals(ApplicationEntity.Status.RUNNING, status);
    }

    private SiteEntity generateSiteEntity() {
        SiteEntity siteEntity = new SiteEntity();
        siteEntity.setSiteId("sandbox");
        siteEntity.setSiteName("sandboxname");
        siteEntity.setDescription("sandboxdesc");
        return siteEntity;
    }

    private ApplicationEntity generateCommonEntity() {
        ApplicationDesc applicationDesc = new ApplicationDesc();
        ApplicationDependency applicationDependency = new ApplicationDependency();
        applicationDependency.setRequired(false);
        applicationDesc.setType("TEST_APPLICATION");
        applicationDesc.setDependencies(new ArrayList<ApplicationDependency>(){{
            add(applicationDependency);
        }});
        ApplicationEntity applicationEntity = new ApplicationEntity();
        applicationEntity.setSite(generateSiteEntity());
        applicationEntity.setDescriptor(applicationDesc);
        applicationEntity.setMode(ApplicationEntity.Mode.LOCAL);
        applicationEntity.setJarPath(applicationDesc.getJarPath());
        Map<String, Object> configure = new HashedMap();
        configure.put("a", "b");
        applicationEntity.setConfiguration(configure);
        applicationEntity.setContext(configure);
        applicationEntity.setAppId("testAppId");
        return applicationEntity;
    }

    private ApplicationEntity generateRunningEntity() {
        ApplicationEntity applicationEntity = generateCommonEntity();
        applicationEntity.setStatus(ApplicationEntity.Status.RUNNING);
        return applicationEntity;
    }
}
