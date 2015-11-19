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
package org.apache.eagle.storage.spi;

import org.apache.eagle.storage.exception.IllegalDataStorageTypeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * @since 3/20/15
 */
public class DataStorageServiceLoader {
    private final Logger LOG = LoggerFactory.getLogger(DataStorageServiceLoader.class);
    private final ServiceLoader<DataStorageServiceProvider> serviceLoader;
    private final Map<String,DataStorageServiceProvider> storageServiceProviders;

    private DataStorageServiceLoader(){
        serviceLoader = ServiceLoader.load(DataStorageServiceProvider.class);
        storageServiceProviders = new HashMap<String,DataStorageServiceProvider>();

        // Load storage providers
        load();
    }

    private static DataStorageServiceLoader instance;
    public static DataStorageServiceLoader getInstance(){
        if(instance == null){
            instance = new DataStorageServiceLoader();
        }
        return instance;
    }

    private void load(){
        Iterator<DataStorageServiceProvider> dataStorageServiceLoaders =  serviceLoader.iterator();
        while(dataStorageServiceLoaders.hasNext()){
            DataStorageServiceProvider provider = dataStorageServiceLoaders.next();
            String storageType = provider.getType();

            if(storageServiceProviders.containsKey(storageType)) {
                LOG.warn("Overrode storage provider: type =" + storageType + ",  provider =  " + provider);
            }else if(storageType == null){
                LOG.error("Loaded storage provider: type = null , provider = " + provider);
                throw new IllegalArgumentException("storage type is null from provider: "+provider);
            }else{
                LOG.info("Loaded storage provider: type = " + storageType + ", provider = " + provider);
            }
            this.storageServiceProviders.put(storageType, provider);
        }

        LOG.info("Successfully loaded storage engines: "+this.getStorageTypes());
    }

    /**
     * Get supported storage types
     *
     * @return supported storage types
     */
    public Set<String> getStorageTypes(){
        return this.storageServiceProviders.keySet();
    }

    /**
     * Reload storage providers
     */
    @SuppressWarnings("unused")
    public void reload(){
        serviceLoader.reload();
        storageServiceProviders.clear();
        load();
    }

    public DataStorageServiceProvider getStorageProviderByType(String type) throws IllegalDataStorageTypeException {
        if(!storageServiceProviders.containsKey(type)){
            throw new IllegalDataStorageTypeException("unknown storage type: "+type+", support: "+this.getStorageTypes());
        }
        return storageServiceProviders.get(type);
    }
}