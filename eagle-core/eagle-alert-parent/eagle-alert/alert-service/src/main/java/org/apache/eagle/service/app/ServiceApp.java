/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.eagle.service.app;

import io.dropwizard.Application;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import io.swagger.jaxrs.config.BeanConfig;
import io.swagger.jaxrs.listing.ApiListingResource;

import java.util.EnumSet;

import javax.servlet.DispatcherType;

import org.apache.commons.lang3.StringUtils;
import org.apache.eagle.alert.coordinator.CoordinatorListener;
import org.apache.eagle.alert.coordinator.resource.CoordinatorResource;
import org.apache.eagle.alert.resource.SimpleCORSFiler;
import org.apache.eagle.service.metadata.resource.MetadataResource;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.typesafe.config.ConfigFactory;

/**
 * @since Jun 27, 2016
 */
public class ServiceApp extends Application<AlertDropWizardConfiguration> {

    public static void main(String[] args) throws Exception {
        new ServiceApp().run(args);
    }

    @Override
    public String getName() {
        return "alert-engine metadata server and coordinator server!";
    }

    @Override
    public void initialize(Bootstrap<AlertDropWizardConfiguration> bootstrap) {
    }

    @Override
    public void run(AlertDropWizardConfiguration configuration, Environment environment) throws Exception {
        if (StringUtils.isNotEmpty(configuration.getApplicationConfPath())) {
            // setup config if given
            System.setProperty("config.resource", configuration.getApplicationConfPath());
            ConfigFactory.invalidateCaches();
            ConfigFactory.load();
        }

        environment.getApplicationContext().setContextPath("/rest");
        environment.getObjectMapper().setSerializationInclusion(JsonInclude.Include.NON_NULL);

        environment.jersey().register(MetadataResource.class);
        environment.jersey().register(CoordinatorResource.class);

        // swagger resources
        environment.jersey().register(new ApiListingResource());
        BeanConfig swaggerConfig = new BeanConfig();
        swaggerConfig.setTitle("Alert engine service: metadata and coordinator");
        swaggerConfig.setVersion("v1.2");
        swaggerConfig.setBasePath("/rest");
        swaggerConfig
            .setResourcePackage("org.apache.eagle.alert.coordinator.resource,org.apache.eagle.service.metadata.resource");
        swaggerConfig.setScan(true);

        // simple CORS filter
        environment.servlets().addFilter("corsFilter", new SimpleCORSFiler())
            .addMappingForUrlPatterns(EnumSet.of(DispatcherType.REQUEST), true, "/*");

        // context listener
        environment.servlets().addServletListeners(new CoordinatorListener());
    }

}
