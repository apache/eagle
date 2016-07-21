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
package org.apache.eagle.app.service.loader;

import com.google.common.base.Preconditions;
import com.typesafe.config.Config;
import org.apache.eagle.app.config.ApplicationProviderConfig;
import org.apache.eagle.app.config.ApplicationProvidersConfig;
import org.apache.eagle.app.service.ApplicationProviderLoader;
import org.apache.eagle.app.spi.ApplicationProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.Unmarshaller;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;

public class ApplicationProviderConfigLoader extends ApplicationProviderLoader {
    public final static String DEFAULT_APPLICATIONS_CONFIG_FILE = "providers.xml";
    private final static String APPLICATIONS_CONFIG_PROPS_KEY = "application.provider.config";
    private final static Logger LOG = LoggerFactory.getLogger(ApplicationProviderConfigLoader.class);
    public ApplicationProviderConfigLoader(Config config) {
        super(config);
    }

    @Override
    public void load() {
        List<ApplicationProviderConfig> applicationProviderConfigs = loadProvidersFromProvidersConf();
        int totalCount = applicationProviderConfigs.size();
        int loadedCount = 0,failedCount = 0;
        for(ApplicationProviderConfig providerConfig: applicationProviderConfigs){
            try {
                initializeProvider(providerConfig);
                loadedCount ++;
            }catch (Throwable ex){
                LOG.warn("Failed to initialized {}, ignored",providerConfig,ex);
                failedCount ++;
            }
        }
        LOG.info("Loaded {} app providers (total: {}, failed: {})",loadedCount,totalCount,failedCount);
    }

    public static boolean appProviderConfExists(String applicationConfFile){
        InputStream is = ApplicationProviderConfigLoader.class.getResourceAsStream(applicationConfFile);
        if(is == null){
            is = ApplicationProviderConfigLoader.class.getResourceAsStream("/"+applicationConfFile);
        }

        if(is != null){
            try {
                return true;
            } finally {
                try {
                    is.close();
                } catch (IOException e) {
                    LOG.debug(e.getMessage());
                }
            }
        } else {
            return false;
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
        provider.prepare(providerConfig,this.getConfig());
        Preconditions.checkNotNull(provider.getApplicationDesc(),"appDesc is null");
        Preconditions.checkNotNull(provider.getApplicationDesc().getType(),"type is null");
        registerProvider(provider);
    }

    private List<ApplicationProviderConfig> loadProvidersFromProvidersConf() {
        String providerConfigFile = DEFAULT_APPLICATIONS_CONFIG_FILE;
        if(getConfig().hasPath(APPLICATIONS_CONFIG_PROPS_KEY)){
            providerConfigFile = getConfig().getString(APPLICATIONS_CONFIG_PROPS_KEY);
            LOG.info("Set {} = {}",APPLICATIONS_CONFIG_PROPS_KEY,providerConfigFile);
        }
        InputStream is = null;
        try {
            JAXBContext jc = JAXBContext.newInstance(ApplicationProvidersConfig.class);
            Unmarshaller unmarshaller = jc.createUnmarshaller();
            is = ApplicationProviderConfigLoader.class.getResourceAsStream(providerConfigFile);
            if(is == null){
                is = ApplicationProviderConfigLoader.class.getResourceAsStream("/"+providerConfigFile);
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
}