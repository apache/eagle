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

import com.codahale.metrics.health.HealthCheckRegistry;
import com.google.inject.Injector;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.dropwizard.setup.Environment;
import org.apache.commons.collections.map.HashedMap;
import org.apache.eagle.app.service.impl.ApplicationHealthCheckServiceImpl;
import org.apache.eagle.app.service.impl.ApplicationProviderServiceImpl;
import org.apache.eagle.metadata.model.ApplicationDesc;
import org.apache.eagle.metadata.model.ApplicationEntity;
import org.apache.eagle.metadata.model.SiteEntity;
import org.apache.eagle.metadata.service.ApplicationEntityService;
import org.junit.Before;
import org.junit.Test;
import org.powermock.reflect.Whitebox;

import java.util.*;

import static org.mockito.Mockito.*;

/**
 * @Since 11/24/16.
 */
public class TestApplicationHealthCheckServiceImpl {

    ApplicationProviderService applicationProviderService;
    ApplicationEntityService applicationEntityService;
    Environment environment;
    Config config;
    ApplicationHealthCheckServiceImpl applicationHealthCheckService;

    @Before
    public void setUp() {
        applicationEntityService = mock(ApplicationEntityService.class);
        environment = mock(Environment.class);
        config = ConfigFactory.parseMap(new HashMap<String, String>() {
            {
                put("application.healthCheck.initialDelay", "10000");
                put("application.healthCheck.period", "10000");
                put("application.healthCheck.publisher.publisherImpl", "org.apache.eagle.app.service.impl.ApplicationHealthCheckEmailPublisher");
                put("application.healthCheck.publisher.dailySendHour", "11");
                put("service.timezone", "UTC");
                put("application.provider.loader", "org.apache.eagle.app.service.impl.ApplicationProviderSPILoader");
            }
        });
        applicationProviderService = new ApplicationProviderServiceImpl(config);
        HealthCheckRegistry healthCheckRegistry = new HealthCheckRegistry();
        when(environment.healthChecks()).thenReturn(healthCheckRegistry);
        when(applicationEntityService.findAll()).thenReturn(generateCollections());
        applicationHealthCheckService = new ApplicationHealthCheckServiceImpl(applicationProviderService, applicationEntityService, config);
        Injector injector = mock(Injector.class);
        Whitebox.setInternalState(applicationHealthCheckService, "currentInjector", injector);

    }

    @Test
    public void testInit() throws NoSuchFieldException, IllegalAccessException {
        applicationHealthCheckService.init(environment);
        applicationHealthCheckService.unregister((ApplicationEntity)((List)generateCollections()).get(0));
    }

    public Collection<ApplicationEntity> generateCollections() {
        Collection<ApplicationEntity> entities = new ArrayList<>();
        SiteEntity siteEntity = new SiteEntity();
        siteEntity.setSiteId("testsiteid");
        siteEntity.setSiteName("testsitename");
        siteEntity.setDescription("testdesc");
        ApplicationDesc applicationDesc = new ApplicationDesc();
        applicationDesc.setType("TEST_APPLICATION");
        ApplicationEntity applicationEntity = new ApplicationEntity();
        applicationEntity.setSite(siteEntity);
        applicationEntity.setDescriptor(applicationDesc);
        applicationEntity.setMode(ApplicationEntity.Mode.LOCAL);
        applicationEntity.setJarPath(applicationDesc.getJarPath());
        Map<String, Object> configure = new HashedMap();
        configure.put("a", "b");
        applicationEntity.setConfiguration(configure);
        applicationEntity.setContext(configure);
        applicationEntity.setAppId("appId");
        entities.add(applicationEntity);
        return entities;
    }
}
