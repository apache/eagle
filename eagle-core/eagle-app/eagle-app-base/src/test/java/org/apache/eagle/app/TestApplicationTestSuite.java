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

import com.google.inject.Inject;
import org.apache.eagle.app.resource.ApplicationResource;
import org.apache.eagle.app.service.AppOperations;
import org.apache.eagle.app.test.ApplicationSimulator;
import org.apache.eagle.app.test.AppUnitTestRunner;
import org.apache.eagle.metadata.model.ApplicationDesc;
import org.apache.eagle.metadata.model.ApplicationEntity;
import org.apache.eagle.metadata.model.SiteEntity;
import org.apache.eagle.metadata.resource.SiteResource;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Collection;

@RunWith(AppUnitTestRunner.class)
public class TestApplicationTestSuite {
    @Inject private SiteResource siteResource;
    @Inject private ApplicationResource applicationResource;
    @Inject private ApplicationSimulator simulator;

    @Test
    public void testApplicationProviderLoading(){
        Collection<ApplicationDesc> applicationDescs = applicationResource.getApplicationDescs();
        Assert.assertNotNull(applicationDescs);
        Assert.assertEquals(1,applicationDescs.size());
    }

    @Test
    public void testApplicationLifecycle() throws InterruptedException {
        // Create local site
        SiteEntity siteEntity = new SiteEntity();
        siteEntity.setSiteId("test_site");
        siteEntity.setSiteName("Test Site");
        siteEntity.setDescription("Test Site for ExampleApplicationTest");
        siteResource.createSite(siteEntity);
        Assert.assertNotNull(siteEntity.getUuid());

        // Install application
        ApplicationEntity applicationEntity = applicationResource.installApplication(new AppOperations.InstallOperation("test_site","TEST_APPLICATION", ApplicationEntity.Mode.LOCAL));
        // Start application
        applicationResource.startApplication(new AppOperations.StartOperation(applicationEntity.getUuid()));
        // Stop application
        applicationResource.stopApplication(new AppOperations.StopOperation(applicationEntity.getUuid()));
        // Uninstall application
        applicationResource.uninstallApplication(new AppOperations.UninstallOperation(applicationEntity.getUuid()));
        try {
            applicationResource.getApplicationEntityByUUID(applicationEntity.getUuid());
            Assert.fail("Application instance (UUID: "+applicationEntity.getUuid()+") should have been uninstalled");
        } catch (Exception ex){
            // Expected exception
        }
    }

    @Test
    public void testApplicationQuickRunWithAppType(){
        simulator.submit("TEST_APPLICATION");
    }

    @Test
    public void testApplicationQuickRunWithAppProvider(){
        simulator.submit(TestApplicationImpl.Provider.class);
    }
}