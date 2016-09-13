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

import static org.junit.Assert.*;

import com.google.inject.Inject;
import org.apache.eagle.app.service.ApplicationProviderService;
import org.apache.eagle.app.spi.ApplicationProvider;
import org.apache.eagle.app.test.ApplicationTestBase;
import org.apache.eagle.metadata.model.ApplicationDesc;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;

public class ApplicationProviderServiceTest extends ApplicationTestBase {
    private final static Logger LOGGER = LoggerFactory.getLogger(ApplicationProviderServiceTest.class);

    @Inject
    private
    ApplicationProviderService providerManager;

    @Test
    public void testApplicationProviderLoaderService(){
        Collection<ApplicationDesc> applicationDescs = providerManager.getApplicationDescs();
        Collection<ApplicationProvider> applicationProviders = providerManager.getProviders();
        applicationDescs.forEach((d)-> LOGGER.debug(d.toString()));
        applicationProviders.forEach((d)-> LOGGER.debug(d.toString()));
        assertNull(providerManager.getApplicationDescByType("TEST_APPLICATION").getViewPath());
        assertEquals("/apps/test_web_app",providerManager.getApplicationDescByType("TEST_WEB_APPLICATION").getViewPath());
        assertNotNull(providerManager.getApplicationDescByType("TEST_WEB_APPLICATION").getDependencies());
        assertEquals(1,providerManager.getApplicationDescByType("TEST_WEB_APPLICATION").getDependencies().size());
        assertEquals("TEST_APPLICATION",providerManager.getApplicationDescByType("TEST_WEB_APPLICATION").getDependencies().get(0).getType());
    }
}