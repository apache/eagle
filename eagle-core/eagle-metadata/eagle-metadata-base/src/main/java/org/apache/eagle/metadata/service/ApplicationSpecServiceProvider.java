/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

package org.apache.eagle.metadata.service;

import com.google.inject.Inject;
import com.google.inject.Provider;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.eagle.metadata.model.ApplicationSpec;
import org.apache.eagle.metadata.model.ApplicationsConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.Unmarshaller;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ApplicationSpecServiceProvider implements Provider<ApplicationSpecService> {

    private final Config config;

    @Inject
    public ApplicationSpecServiceProvider(Config config){
        this.config = config;
    }

    @Override
    public ApplicationSpecService get() {
        return new ApplicationSpecServiceImpl(config);
    }

    private static class ApplicationSpecServiceImpl implements ApplicationSpecService {
        public ApplicationSpecServiceImpl(Config config){
            load(config);
        }

        private ApplicationsConfig applicationsConfig;
        private final static String DEFAULT_APPLICATIONS_CONFIG_FILE = "applications.xml";
        private final static String APPLICATIONS_CONFIG_PROPS_KEY = "applications.config";
        private final static Logger LOG = LoggerFactory.getLogger(ApplicationSpecServiceImpl.class);
        private final static Map<String,ApplicationSpec> APPLICATION_TYPE_SPEC_MAP = new HashMap<>();

        private void load(Config config) {
            try {
                APPLICATION_TYPE_SPEC_MAP.clear();
                String applicationsConfigFile = DEFAULT_APPLICATIONS_CONFIG_FILE;
                if(config.hasPath(APPLICATIONS_CONFIG_PROPS_KEY)){
                    applicationsConfigFile = config.getString(APPLICATIONS_CONFIG_PROPS_KEY);
                    LOG.info("Set {} = {}",APPLICATIONS_CONFIG_PROPS_KEY,applicationsConfigFile);
                }
                JAXBContext jc = JAXBContext.newInstance(ApplicationsConfig.class);
                Unmarshaller unmarshaller = jc.createUnmarshaller();
                InputStream is = ApplicationSpecServiceImpl.class.getResourceAsStream(applicationsConfigFile);
                if(is == null){
                    is = ApplicationSpecServiceImpl.class.getResourceAsStream("/"+applicationsConfigFile);
                }
                if(is == null){
                    LOG.error("Application configuration {} is not found",applicationsConfigFile);
                }
                applicationsConfig = (ApplicationsConfig) unmarshaller.unmarshal(is);
                for(ApplicationSpec applicationSpec:applicationsConfig.getApplications()){
                    APPLICATION_TYPE_SPEC_MAP.put(applicationSpec.getType(),applicationSpec);
                }
                LOG.info("Loaded {} application specs",applicationsConfig.getApplications().size());
            }catch (Exception ex){
                LOG.error("Failed to load application configuration: applications.xml",ex);
                throw new RuntimeException("Failed to load application configuration: applications.xml",ex);
            }
        }

        @Override
        public ApplicationSpec getApplicationSpecByType(String appType) {
            return APPLICATION_TYPE_SPEC_MAP.get(appType);
        }

        @Override
        public List<ApplicationSpec> getAllApplicationSpecs() {
            return applicationsConfig.getApplications();
        }
    }
}