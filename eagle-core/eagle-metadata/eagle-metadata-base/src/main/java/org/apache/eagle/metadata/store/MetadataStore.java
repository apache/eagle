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
package org.apache.eagle.metadata.store;

import com.google.inject.AbstractModule;
import com.google.inject.Singleton;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.eagle.metadata.service.ApplicationSpecService;
import org.apache.eagle.metadata.service.ApplicationSpecServiceProvider;
import org.apache.eagle.metadata.service.ConfigServiceProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class MetadataStore extends AbstractModule {
    private final static Logger LOG = LoggerFactory.getLogger(MetadataStore.class);
    public static final String METADATA_STORE_CONFIG_KEY = "metadata.store";

    private static MetadataStore instance;
    public static MetadataStore getInstance(){
        if(instance == null) {
            try {
                Config config = ConfigFactory.load();
                String metadataStoreClass;
                if (config.hasPath(METADATA_STORE_CONFIG_KEY)) {
                    metadataStoreClass = config.getString(METADATA_STORE_CONFIG_KEY);
                    LOG.info("Using {} = {}",METADATA_STORE_CONFIG_KEY,metadataStoreClass);
                }else{
                    metadataStoreClass = MemoryMetadataStore.class.getCanonicalName();
                    LOG.info("{} is not set, using default {}",METADATA_STORE_CONFIG_KEY,metadataStoreClass);
                }
                instance = (MetadataStore) Class.forName(metadataStoreClass).newInstance();
            } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
                throw new RuntimeException(e.getMessage(), e.getCause());
            }
        }
        return instance;
    }

    @Override
    protected void configure() {
        bind(Config.class).toProvider(ConfigServiceProvider.class).in(Singleton.class);
        bind(ApplicationSpecService.class).toProvider(ApplicationSpecServiceProvider.class).in(Singleton.class);
    }
}