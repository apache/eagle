/**
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
package org.apache.eagle.app.base;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.eagle.app.base.metadata.ApplicationInstance;
import org.apache.eagle.app.base.metadata.ApplicationSpec;
import org.apache.eagle.app.base.metadata.ApplicationsConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.Unmarshaller;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

class ApplicationManagerImpl extends ApplicationManager{
    private ApplicationsConfig applicationsConfig;
    private final static String DEFAULT_APPLICATIONS_CONFIG_FILE = "applications.xml";
    private final static String APPLICATIONS_CONFIG_PROPS_KEY = "applications.config";
    private final static Logger LOG = LoggerFactory.getLogger(ApplicationManagerImpl.class);

    private final static Map<String,Application> APPLICATION_INSTANCE_CACHE = new HashMap<>();
    private final static Map<String,ApplicationSpec> APPLICATION_TYPE_SPEC_MAP = new HashMap<>();

    private static Application getApplicationInstance(ApplicationSpec applicationSpec) {
        String applicationClassName = applicationSpec.getClassname();
        if(APPLICATION_INSTANCE_CACHE.containsKey(applicationClassName)){
            return APPLICATION_INSTANCE_CACHE.get(applicationClassName);
        }
        LOG.debug("Initializing new instance for {}",applicationClassName);
        try {
            Application instance  = (Application) Class.forName(applicationClassName).newInstance();
            instance.onInit();
            APPLICATION_INSTANCE_CACHE.put(applicationClassName,instance);
            return instance;
        } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
            LOG.error("Failed to initialize application {}",applicationClassName,e);
            throw new RuntimeException("Failed to initialize application from class "+applicationClassName,e);
        }
    }

    @Override
    public synchronized void load() {
        try {
            APPLICATION_INSTANCE_CACHE.clear();
            APPLICATION_TYPE_SPEC_MAP.clear();
            Config config = ConfigFactory.load();
            String applicationsConfigFile = DEFAULT_APPLICATIONS_CONFIG_FILE;
            if(config.hasPath(APPLICATIONS_CONFIG_PROPS_KEY)){
                applicationsConfigFile = config.getString(APPLICATIONS_CONFIG_PROPS_KEY);
                LOG.info("Set {} = {}",APPLICATIONS_CONFIG_PROPS_KEY,applicationsConfigFile);
            }
            JAXBContext jc = JAXBContext.newInstance(ApplicationsConfig.class);
            Unmarshaller unmarshaller = jc.createUnmarshaller();
            InputStream is = ApplicationManagerImpl.class.getResourceAsStream(applicationsConfigFile);
            if(is == null){
                is = ApplicationManagerImpl.class.getResourceAsStream("/"+applicationsConfigFile);
            }
            if(is == null){
                LOG.error("Application configuration {} is not found",applicationsConfigFile);
            }
            applicationsConfig = (ApplicationsConfig) unmarshaller.unmarshal(is);
            for(ApplicationSpec applicationSpec:applicationsConfig.getApplications()){
                APPLICATION_TYPE_SPEC_MAP.put(applicationSpec.getType(),applicationSpec);
            }
        }catch (Exception ex){
            LOG.error("Failed to load application configuration: applications.xml",ex);
            throw new RuntimeException("Failed to load application configuration: applications.xml",ex);
        }
    }

    @Override
    public List<ApplicationSpec> getAllApplications() {
        return applicationsConfig.getApplications();
    }

    @Override
    public void startApp(ApplicationInstance instance) {
        getApplicationInstance(instance.getApplication()).onStart(instance);
    }

    @Override
    public void stopApp(ApplicationInstance instance) {
        getApplicationInstance(instance.getApplication()).onStop(instance);
    }

    @Override
    public void installApp(ApplicationInstance instance) {
        getApplicationInstance(instance.getApplication()).onInstall(instance);
    }

    @Override
    public void uninstallApp(ApplicationInstance instance) {
        getApplicationInstance(instance.getApplication()).onUninstall(instance);
    }

    @Override
    public ApplicationSpec getApplicationByType(String appType) {
        return APPLICATION_TYPE_SPEC_MAP.get(appType);
    }
}