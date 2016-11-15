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

import com.codahale.metrics.health.HealthCheck;
import com.google.inject.Inject;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.dropwizard.setup.Environment;
import org.apache.eagle.app.service.ApplicationHealthCheckService;
import org.apache.eagle.app.service.ApplicationProviderService;
import org.apache.eagle.app.spi.ApplicationProvider;
import org.apache.eagle.metadata.model.ApplicationEntity;
import org.apache.eagle.metadata.service.ApplicationEntityService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;

public class ApplicationHealthCheckServiceImpl implements ApplicationHealthCheckService {
    private static final Logger LOG = LoggerFactory.getLogger(ApplicationHealthCheckServiceImpl.class);

    private final ApplicationProviderService applicationProviderService;
    private final ApplicationEntityService applicationEntityService;
    private final Config config;
    private Environment environment;

    @Inject
    public ApplicationHealthCheckServiceImpl(ApplicationProviderService applicationProviderService,
                                             ApplicationEntityService applicationEntityService,
                                             Config config) {
        this.applicationProviderService = applicationProviderService;
        this.applicationEntityService = applicationEntityService;
        this.config = config;
    }

    @Override
    public void init(Environment environment) {
        this.environment = environment;
        Collection<ApplicationEntity> applicationEntities = applicationEntityService.findAll();
        applicationEntities.forEach(this::register);
    }

    @Override
    public void register(ApplicationEntity appEntity) {
        ApplicationProvider<?> appProvider = applicationProviderService.getApplicationProviderByType(appEntity.getDescriptor().getType());
        HealthCheck applicationHealthCheck = appProvider.getApplication().getAppHealthCheck(
                ConfigFactory.parseMap(appEntity.getConfiguration())
                        .withFallback(config)
                        .withFallback(ConfigFactory.parseMap(appEntity.getContext()))
        );
        this.environment.healthChecks().register(appEntity.getAppId(), applicationHealthCheck);
        LOG.info("successfully register health check for {}", appEntity.getAppId());
    }

    @Override
    public void unregister(ApplicationEntity appEntity) {
        this.environment.healthChecks().unregister(appEntity.getAppId());
        LOG.info("successfully unregister health check for {}", appEntity.getAppId());
    }
}
