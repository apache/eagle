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
package org.apache.eagle.app.service;

import com.google.common.base.Preconditions;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.typesafe.config.Config;
import org.apache.eagle.app.spi.ApplicationProvider;
import org.apache.eagle.app.config.ApplicationProviderConfig;
import org.apache.eagle.app.config.ApplicationProvidersConfig;
import org.apache.eagle.metadata.model.ApplicationDesc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.Unmarshaller;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Support to load application provider from application.provider.config = "providers.xml" configuration file
 * or application.provider.dir = "lib/apps" with SPI Class loader
 *
 * TODO: hot-manage application provider loading
 */
@Singleton
public class ApplicationProviderServiceImpl implements ApplicationProviderService {
    private final Config config;
    private final Map<String,ApplicationProvider> providers;
    private final static String DEFAULT_APPLICATIONS_CONFIG_FILE = "providers.xml";
    private final static String APPLICATIONS_CONFIG_PROPS_KEY = "application.provider.config";
    private final static String APPLICATIONS_DIR_PROPS_KEY = "application.provider.dir";
    private final static Logger LOG = LoggerFactory.getLogger(ApplicationProviderServiceImpl.class);

    @Inject
    public ApplicationProviderServiceImpl(Config config){
        LOG.info("Initializing {}",this.getClass().getCanonicalName());
        this.config = config;
        this.providers = new HashMap<>();
        reload();
    }

    public synchronized void reload(){
        providers.clear();
        LOG.info("Loading application providers");
        loadProviders();
        LOG.info("Loaded {} application providers",providers.size());
    }

    private void loadProviders(){
        if(config.hasPath(APPLICATIONS_DIR_PROPS_KEY)){
            loadProvidersBySPILoader();
        }else {
            loadProvidersFromConf();
        }
    }

    private void loadProvidersBySPILoader() {
        String appProviderDir = config.getString(APPLICATIONS_DIR_PROPS_KEY);
        LOG.info("Loading application providers from {} with ServiceLoader",appProviderDir);
        File loc = new File(appProviderDir);
        File[] jarFiles = loc.listFiles(file -> file.getPath().toLowerCase().endsWith(".jar"));
        if(jarFiles != null) {
            for (File jarFile : jarFiles) {
                try {
                    URL jarFileUrl = jarFile.toURI().toURL();
                    LOG.debug("Loading ApplicationProvider from jar: {}",jarFileUrl.toString());
                    URLClassLoader jarFileClassLoader = new URLClassLoader(new URL[]{jarFileUrl});
                    ServiceLoader<ApplicationProvider> serviceLoader = ServiceLoader.load(ApplicationProvider.class, jarFileClassLoader);
                    for (ApplicationProvider applicationProvider : serviceLoader) {
                        ApplicationProviderConfig providerConfig = new ApplicationProviderConfig();
                        providerConfig.setJarPath(jarFileUrl.getPath());
                        providerConfig.setClassName(applicationProvider.getClass().getCanonicalName());
                        applicationProvider.prepare(providerConfig,config);
                        registerProvider(applicationProvider);
                    }
                } catch (MalformedURLException e) {
                    LOG.error("Failed to get URL of {}", jarFile);
                }
            }
        }
    }

    private void registerProvider(ApplicationProvider provider){
        if(providers.containsKey(provider.getApplicationDesc().getType())){
            throw new RuntimeException("Duplicated APPLICATION_TYPE: "+provider.getApplicationDesc().getType()+", was already registered by provider: "+providers.get(provider.getApplicationDesc().getType()));
        }
        providers.put(provider.getApplicationDesc().getType(),provider);
        LOG.info("Initialized application provider: {}",provider);
    }

    private void loadProvidersFromConf() {
        List<ApplicationProviderConfig> applicationProviderConfigs = loadProvidersFromProvidersConf();
        for(ApplicationProviderConfig providerConfig: applicationProviderConfigs){
            try {
                initializeProvider(providerConfig);
            }catch (Exception ex){
                LOG.error("Failed to initialized {}, ignored",providerConfig,ex);
            }
        }
    }

    private void initializeProvider(ApplicationProviderConfig providerConfig) throws ClassNotFoundException, IllegalAccessException, InstantiationException {
        LOG.info("Loading application provider {} from {}",providerConfig.getClassName(),providerConfig.getJarPath());
        String providerClassName = providerConfig.getClassName();
        if(providerClassName == null) throw new RuntimeException("provider.classname is null: "+providerConfig);
        if(providerConfig.getJarPath() == null) throw new RuntimeException("provider.jarpath is null: "+providerConfig);

        Class<?> providerClass = Class.forName(providerClassName);

        if(!ApplicationProvider.class.isAssignableFrom(providerClass)){
            throw new RuntimeException("providerClassName is not implementation of "+ApplicationProvider.class.getCanonicalName());
        }
        ApplicationProvider provider = (ApplicationProvider) providerClass.newInstance();
        provider.prepare(providerConfig,this.config);
        Preconditions.checkNotNull(provider.getApplicationDesc(),"appDesc is null");
        Preconditions.checkNotNull(provider.getApplicationDesc().getType(),"type is null");
        registerProvider(provider);
    }

    private List<ApplicationProviderConfig> loadProvidersFromProvidersConf() {
        String providerConfigFile = DEFAULT_APPLICATIONS_CONFIG_FILE;
        if(config.hasPath(APPLICATIONS_CONFIG_PROPS_KEY)){
            providerConfigFile = config.getString(APPLICATIONS_CONFIG_PROPS_KEY);
            LOG.info("Set {} = {}",APPLICATIONS_CONFIG_PROPS_KEY,providerConfigFile);
        }
        InputStream is = null;
        try {
            JAXBContext jc = JAXBContext.newInstance(ApplicationProvidersConfig.class);
            Unmarshaller unmarshaller = jc.createUnmarshaller();
            is = ApplicationProviderServiceImpl.class.getResourceAsStream(providerConfigFile);
            if(is == null){
                is = ApplicationProviderServiceImpl.class.getResourceAsStream("/"+providerConfigFile);
            }
            if(is == null){
                LOG.error("Application provider configuration {} is not found",providerConfigFile);
            }
            Preconditions.checkNotNull(is,providerConfigFile+" is not found");
            return ((ApplicationProvidersConfig) unmarshaller.unmarshal(is)).getProviders();
        }catch (Exception ex){
            LOG.error("Failed to load application provider configuration: {}",providerConfigFile,ex);
            throw new RuntimeException("Failed to load application provider configuration: "+providerConfigFile,ex);
        } finally {
            if(is != null) try {
                is.close();
            } catch (IOException e) {
                LOG.error(e.getMessage(),e);
            }
        }
    }

    public Collection<ApplicationProvider> getProviders(){
        return providers.values();
    }

    public Collection<ApplicationDesc> getApplicationDescs(){
        return getProviders().stream().map(ApplicationProvider::getApplicationDesc).collect(Collectors.toList());
    }

    public ApplicationProvider<?> getApplicationProviderByType(String type) {
        return providers.get(type);
    }

    public ApplicationDesc getApplicationDescByType(String appType) {
        return providers.get(appType).getApplicationDesc();
    }
}