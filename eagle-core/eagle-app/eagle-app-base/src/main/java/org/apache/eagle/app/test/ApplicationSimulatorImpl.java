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
package org.apache.eagle.app.test;

import com.google.inject.Inject;
import com.typesafe.config.Config;
import org.apache.eagle.app.resource.ApplicationResource;
import org.apache.eagle.app.service.ApplicationOperations;
import org.apache.eagle.app.spi.ApplicationProvider;
import org.apache.eagle.metadata.model.ApplicationEntity;
import org.apache.eagle.metadata.model.SiteEntity;
import org.apache.eagle.metadata.resource.SiteResource;
import org.apache.eagle.metadata.service.ApplicationStatusUpdateService;
import org.junit.Assert;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class ApplicationSimulatorImpl extends ApplicationSimulator {
    private final Config config;
    private final SiteResource siteResource;
    private final ApplicationResource applicationResource;

    @Inject
    ApplicationStatusUpdateService statusUpdateService;

    @Inject
    public ApplicationSimulatorImpl(Config config, SiteResource siteResource, ApplicationResource applicationResource) {
        this.config = config;
        this.siteResource = siteResource;
        this.applicationResource = applicationResource;
    }

    private static final AtomicInteger incr = new AtomicInteger();

    private SiteEntity getUniqueSite() {
        // Create local site
        SiteEntity siteEntity = new SiteEntity();
        siteEntity.setSiteId("SIMULATED_SITE_" + incr.incrementAndGet());
        siteEntity.setSiteName(siteEntity.getSiteId());
        siteEntity.setDescription("Automatically generated unique simulation site " + siteEntity.getSiteId() + " (simulator: " + this + ")");
        return siteEntity;
    }

    @Override
    public void start(String appType) {
        start(appType, new HashMap<>());
    }

    @Override
    public void start(String appType, Map<String, Object> appConfig) {
        SiteEntity siteEntity = getUniqueSite();
        siteResource.createSite(siteEntity);
        Assert.assertNotNull(siteEntity.getUuid());
        ApplicationOperations.InstallOperation installOperation = new ApplicationOperations.InstallOperation(siteEntity.getSiteId(), appType, ApplicationEntity.Mode.LOCAL);
        installOperation.setConfiguration(appConfig);
        // Install application
        ApplicationEntity applicationEntity = applicationResource.installApplication(installOperation).getData();
        // Start application
        applicationResource.startApplication(new ApplicationOperations.StartOperation(applicationEntity.getUuid()));
        statusUpdateService.updateApplicationEntityStatus(applicationEntity);
        applicationResource.stopApplication(new ApplicationOperations.StopOperation(applicationEntity.getUuid()));
        int attempt = 0;
        while (attempt < 10) {
            attempt++;
            statusUpdateService.updateApplicationEntityStatus(applicationEntity);
            if (applicationEntity.getStatus() == ApplicationEntity.Status.STOPPED
                    || applicationEntity.getStatus() == ApplicationEntity.Status.INITIALIZED) {
                break;
            } else {
                try {
                    Thread.sleep(500);
                } catch (InterruptedException e) {
                    // Ignore
                }
            }
        }
        if (attempt >= 10 ) {
            throw new IllegalStateException("Application status didn't become STOPPED in 10 attempts");
        }
        applicationResource.uninstallApplication(new ApplicationOperations.UninstallOperation(applicationEntity.getUuid()));
    }

    @Override
    public void start(Class<? extends ApplicationProvider> appProviderClass) {
        start(appProviderClass, new HashMap<>());
    }

    @Override
    public void start(Class<? extends ApplicationProvider> appProviderClass, Map<String, Object> appConfig) {
        try {
            ApplicationProvider applicationProvider = appProviderClass.newInstance();
            start(applicationProvider.getApplicationDesc().getType(), appConfig);
        } catch (InstantiationException | IllegalAccessException e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }
}
