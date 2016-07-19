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
package org.apache.eagle.app.manager;

import com.google.common.base.Preconditions;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.typesafe.config.Config;
import org.apache.eagle.app.ApplicationProvider;
import org.apache.eagle.app.config.ApplicationProviderConfig;
import org.apache.eagle.app.config.ApplicationProvidersConfig;
import org.apache.eagle.metadata.model.ApplicationDesc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.Unmarshaller;
import java.io.InputStream;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

@Singleton
public class ApplicationProviderServiceImpl implements ApplicationProviderService {
    private final Config config;

    private ApplicationProvidersConfig providersConfig;

    private final Map<String,ApplicationProvider> providers;

    private final static String DEFAULT_APPLICATIONS_CONFIG_FILE = "providers.xml";
    private final static String APPLICATIONS_CONFIG_PROPS_KEY = "providers.config";
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
        loadProviderConfig(config);
        initializeProviders();
        LOG.info("Loaded {} application providers",providers.size());
    }

    private void initializeProviders() {
        for(ApplicationProviderConfig providerConfig: getProviderConfigs()){
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
        if(providers.containsKey(provider.getApplicationDesc().getType())){
            throw new RuntimeException("Duplicated APPLICATION_TYPE: "+provider.getApplicationDesc().getType()+", was already registered by provider: "+providers.get(provider.getApplicationDesc().getType()));
        }
        providers.put(provider.getApplicationDesc().getType(),provider);
//        applicationDescMap.put(provider.getApplicationDesc().getType(),provider.getApplicationDesc());
        LOG.info("Loaded application provider: {}",provider);
    }

    private void loadProviderConfig(Config config) {
        String providerConfigFile = DEFAULT_APPLICATIONS_CONFIG_FILE;
        if(config.hasPath(APPLICATIONS_CONFIG_PROPS_KEY)){
            providerConfigFile = config.getString(APPLICATIONS_CONFIG_PROPS_KEY);
            LOG.info("Set {} = {}",APPLICATIONS_CONFIG_PROPS_KEY,providerConfigFile);
        }
        try {
            JAXBContext jc = JAXBContext.newInstance(ApplicationProvidersConfig.class);
            Unmarshaller unmarshaller = jc.createUnmarshaller();
            InputStream is = ApplicationProviderServiceImpl.class.getResourceAsStream(providerConfigFile);
            if(is == null){
                is = ApplicationProviderServiceImpl.class.getResourceAsStream("/"+providerConfigFile);
            }
            if(is == null){
                LOG.error("Application provider configuration {} is not found",providerConfigFile);
            }
            Preconditions.checkNotNull(is,providerConfigFile+" is not found");
            providersConfig = (ApplicationProvidersConfig) unmarshaller.unmarshal(is);
            is.close();
        }catch (Exception ex){
            LOG.error("Failed to load application provider configuration: {}",providerConfigFile,ex);
            throw new RuntimeException("Failed to load application provider configuration: "+providerConfigFile,ex);
        }
    }

    public Collection<ApplicationProviderConfig> getProviderConfigs(){
        return providersConfig.getProviders();
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