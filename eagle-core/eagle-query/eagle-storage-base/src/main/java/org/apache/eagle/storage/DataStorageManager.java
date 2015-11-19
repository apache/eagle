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
package org.apache.eagle.storage;

import org.apache.eagle.common.config.EagleConfigFactory;
import org.apache.eagle.storage.spi.DataStorageServiceLoader;
import org.apache.eagle.storage.exception.IllegalDataStorageTypeException;
import org.apache.eagle.storage.spi.DataStorageServiceProvider;
import org.apache.commons.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Properties;

/**
 * @since 3/20/15
 */
public class DataStorageManager {
    public final static String EAGLE_STORAGE_TYPE = "eagle.service.storage-type";
    public final static String DEFAULT_DATA_STORAGE_TYPE = "hbase";

    private final static Logger LOG = LoggerFactory.getLogger(DataStorageManager.class);

    /**
     * Get data storage without cache
     *
     * @param type
     * @throws IllegalDataStorageTypeException
     */
    public static DataStorage newDataStorage(String type) throws IllegalDataStorageTypeException {
        DataStorageServiceProvider serviceProvider = DataStorageServiceLoader.getInstance().getStorageProviderByType(type);
        if(serviceProvider == null){
            throw new IllegalDataStorageTypeException("data storage provider of type: "+type+" is null");
        }
        DataStorage dataStorage =  serviceProvider.getStorage();
        try {
            LOG.info("Initializing data storage engine: "+dataStorage);
            dataStorage.init();
        } catch (IOException e) {
            LOG.error("Failed to initialize data storage engine "+dataStorage,e);
            throw new IllegalStateException(e);
        }
        return dataStorage;
    }

    private static DataStorage singletonStorageInstance;
    /**
     * get storage class by type configured as eagle.storage.type from eagle configuration: config.properties
     *
     * @return DataStorage instance
     *
     * @throws IllegalDataStorageTypeException
     */
    public static DataStorage getDataStorageByEagleConfig(boolean cache) throws IllegalDataStorageTypeException{
        String storageType = EagleConfigFactory.load().getStorageType();

        if(!cache)
            return newDataStorage(storageType);

        if(singletonStorageInstance == null) {
            if (storageType == null) {
                LOG.error(EAGLE_STORAGE_TYPE + " is null, trying default data storage: " + DEFAULT_DATA_STORAGE_TYPE);
                storageType = DEFAULT_DATA_STORAGE_TYPE;
            }
            singletonStorageInstance = newDataStorage(storageType);
        }
        return singletonStorageInstance;
    }

    /**
     * @return DataStorage instance by singleton pattern
     *
     * @throws IllegalDataStorageTypeException
     */
    public static DataStorage getDataStorageByEagleConfig() throws IllegalDataStorageTypeException{
        return getDataStorageByEagleConfig(true);
    }

    /**
     * Get data storage by configuration
     *
     * @param configuration
     * @return
     * @throws IllegalDataStorageTypeException
     */
    public static DataStorage newDataStorage(Configuration configuration) throws IllegalDataStorageTypeException {
        String storageType = configuration.getString(EAGLE_STORAGE_TYPE);
        if(storageType == null){
            throw new IllegalDataStorageTypeException(EAGLE_STORAGE_TYPE+" is null");
        }
        return newDataStorage(storageType);
    }

    /**
     * Get data storage by properties
     *
     * @param properties
     * @return
     * @throws IllegalDataStorageTypeException
     */
    public static DataStorage newDataStorage(Properties properties) throws IllegalDataStorageTypeException {
        String storageType = (String) properties.get(EAGLE_STORAGE_TYPE);
        if(storageType == null){
            LOG.error(EAGLE_STORAGE_TYPE+" is null");
            throw new IllegalDataStorageTypeException(EAGLE_STORAGE_TYPE+" is null");
        }
        return newDataStorage(storageType);
    }
}