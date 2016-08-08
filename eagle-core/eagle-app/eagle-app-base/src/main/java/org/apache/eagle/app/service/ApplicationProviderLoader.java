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

import com.typesafe.config.Config;
import org.apache.eagle.app.service.impl.ApplicationProviderConfigLoader;
import org.apache.eagle.app.service.impl.ApplicationProviderSPILoader;
import org.apache.eagle.app.spi.ApplicationProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public abstract class ApplicationProviderLoader {
    private final Config config;
    private final Map<String,ApplicationProvider> providers;
    private final static Logger LOG = LoggerFactory.getLogger(ApplicationProviderLoader.class);

    public ApplicationProviderLoader(Config config) {
        this.config = config;
        this.providers = new HashMap<>();
    }

    public abstract void load();

    protected Config getConfig() {
        return config;
    }

    protected void registerProvider(ApplicationProvider provider){
        if(providers.containsKey(provider.getApplicationDesc().getType())){
            throw new RuntimeException("Duplicated APPLICATION_TYPE: "+provider.getApplicationDesc().getType()+", was already registered by provider: "+providers.get(provider.getApplicationDesc().getType()));
        }
        providers.put(provider.getApplicationDesc().getType(),provider);
        LOG.info("Initialized application provider: {}",provider);
    }

    public Collection<ApplicationProvider> getProviders(){
        return providers.values();
    }

    public ApplicationProvider<?> getApplicationProviderByType(String type) {
        if(providers.containsKey(type)) {
            return providers.get(type);
        }else{
            throw new IllegalArgumentException("Unknown Application Type: "+type);
        }
    }

    public void reset(){
        providers.clear();
    }

    public static String getDefaultAppProviderLoader(){
        if(ApplicationProviderConfigLoader
                .appProviderConfExists(ApplicationProviderConfigLoader.DEFAULT_APPLICATIONS_CONFIG_FILE)){
            return ApplicationProviderConfigLoader.class.getCanonicalName();
        } else {
            return ApplicationProviderSPILoader.class.getCanonicalName();
        }
    }
}
