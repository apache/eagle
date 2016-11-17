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
package org.apache.eagle.server;

import com.google.inject.Inject;
import com.hubspot.dropwizard.guice.GuiceBundle;
import com.sun.jersey.api.core.PackagesResourceConfig;
import io.dropwizard.Application;
import io.dropwizard.assets.AssetsBundle;
import io.dropwizard.lifecycle.Managed;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import io.swagger.jaxrs.config.BeanConfig;
import io.swagger.jaxrs.listing.ApiListingResource;
import org.apache.eagle.alert.coordinator.CoordinatorListener;
import org.apache.eagle.alert.resource.SimpleCORSFiler;
import org.apache.eagle.app.service.ApplicationHealthCheckService;
import org.apache.eagle.common.Version;
import org.apache.eagle.log.base.taggedlog.EntityJsonModule;
import org.apache.eagle.log.base.taggedlog.TaggedLogAPIEntity;
import org.apache.eagle.metadata.service.ApplicationStatusUpdateService;
import org.apache.eagle.server.authentication.BasicAuthProviderBuilder;
import org.apache.eagle.server.task.ApplicationTask;
import org.apache.eagle.server.module.GuiceBundleLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.DispatcherType;
import java.util.EnumSet;

class ServerApplication extends Application<ServerConfig> {
    private static final Logger LOG = LoggerFactory.getLogger(ServerApplication.class);
    @Inject
    private ApplicationStatusUpdateService applicationStatusUpdateService;
    @Inject
    private ApplicationHealthCheckService applicationHealthCheckService;

    @Override
    public void initialize(Bootstrap<ServerConfig> bootstrap) {
        LOG.debug("Loading and registering guice bundle");
        GuiceBundle<ServerConfig> guiceBundle = GuiceBundleLoader.load();
        bootstrap.addBundle(guiceBundle);

        LOG.debug("Loading and registering static AssetsBundle on /assets");
        bootstrap.addBundle(new AssetsBundle("/assets", "/", "index.html", "/"));

        LOG.debug("Initializing guice injector context for current ServerApplication");
        guiceBundle.getInjector().injectMembers(this);
    }

    @Override
    public String getName() {
        return ServerConfig.getServerName();
    }

    @Override
    public void run(ServerConfig configuration, Environment environment) throws Exception {
        environment.getApplicationContext().setContextPath(ServerConfig.getContextPath());
        environment.jersey().register(RESTExceptionMapper.class);
        environment.jersey().setUrlPattern(ServerConfig.getApiBasePath());
        environment.getObjectMapper().setFilters(TaggedLogAPIEntity.getFilterProvider());
        environment.getObjectMapper().registerModule(new EntityJsonModule());

        // Automatically scan all REST resources
        new PackagesResourceConfig(ServerConfig.getResourcePackage()).getClasses().forEach(environment.jersey()::register);

        // Swagger resources
        environment.jersey().register(ApiListingResource.class);

        BeanConfig swaggerConfig = new BeanConfig();
        swaggerConfig.setTitle(ServerConfig.getServerName());
        swaggerConfig.setVersion(ServerConfig.getServerVersion().version);
        swaggerConfig.setBasePath(ServerConfig.getApiBasePath());
        swaggerConfig.setResourcePackage(ServerConfig.getResourcePackage());
        swaggerConfig.setLicense(ServerConfig.getLicense());
        swaggerConfig.setLicenseUrl(ServerConfig.getLicenseUrl());
        swaggerConfig.setDescription(Version.str());
        swaggerConfig.setScan(true);

        // Simple CORS filter
        environment.servlets().addFilter(SimpleCORSFiler.class.getName(), new SimpleCORSFiler())
            .addMappingForUrlPatterns(EnumSet.of(DispatcherType.REQUEST), true, "/*");

        // Register authentication provider
        environment.jersey().register(new BasicAuthProviderBuilder(configuration.getAuth(), environment).build());

        // Context listener
        environment.servlets().addServletListeners(new CoordinatorListener());

        // Run application status service in background
        Managed updateAppStatusTask = new ApplicationTask(applicationStatusUpdateService);
        environment.lifecycle().manage(updateAppStatusTask);

        // Initialize application health check environment
        applicationHealthCheckService.init(environment);
        Managed appHealthCheckTask = new ApplicationTask(applicationHealthCheckService);
        environment.lifecycle().manage(appHealthCheckTask);
    }
}