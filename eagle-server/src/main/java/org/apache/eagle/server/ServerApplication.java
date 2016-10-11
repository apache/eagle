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
import com.google.inject.Injector;
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
import org.apache.eagle.log.base.taggedlog.EntityJsonModule;
import org.apache.eagle.log.base.taggedlog.TaggedLogAPIEntity;
import org.apache.eagle.metadata.service.ApplicationStatusUpdateService;
import org.apache.eagle.server.authentication.AuthenticationRegister;
import org.apache.eagle.server.authentication.principal.User;
import org.apache.eagle.server.managedtask.ApplicationTask;
import org.apache.eagle.server.module.GuiceBundleLoader;

import javax.servlet.DispatcherType;
import java.util.EnumSet;

class ServerApplication extends Application<ServerConfig> {
    private GuiceBundle guiceBundle;

    @Override
    public void initialize(Bootstrap<ServerConfig> bootstrap) {
        guiceBundle = GuiceBundleLoader.load();
        bootstrap.addBundle(guiceBundle);
        bootstrap.addBundle(new AssetsBundle("/assets", "/", "index.html", "/"));
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
        swaggerConfig.setVersion(ServerConfig.getServerVersion());
        swaggerConfig.setBasePath(ServerConfig.getApiBasePath());
        swaggerConfig.setResourcePackage(ServerConfig.getResourcePackage());
        swaggerConfig.setLicense(ServerConfig.getLicense());
        swaggerConfig.setLicenseUrl(ServerConfig.getLicenseUrl());
        swaggerConfig.setScan(true);

        // Simple CORS filter
        environment.servlets().addFilter(SimpleCORSFiler.class.getName(), new SimpleCORSFiler())
            .addMappingForUrlPatterns(EnumSet.of(DispatcherType.REQUEST), true, "/*");

        // add authentication filters
        new AuthenticationRegister<>(configuration, environment, User.class).register();

        // context listener
        environment.servlets().addServletListeners(new CoordinatorListener());

        // run application status service in background
        Injector injector = guiceBundle.getInjector();
        ApplicationStatusUpdateService applicationStatusUpdateService = injector.getInstance(ApplicationStatusUpdateService.class);
        Managed updateAppStatusTask = new ApplicationTask(applicationStatusUpdateService);
        environment.lifecycle().manage(updateAppStatusTask);
    }
}