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

import org.apache.eagle.storage.exception.IllegalDataStorageException;
import org.apache.eagle.storage.exception.IllegalDataStorageTypeException;
import org.apache.eagle.storage.spi.TestDataStorage;
import org.apache.commons.configuration.AbstractConfiguration;
import org.apache.commons.configuration.CombinedConfiguration;
import org.junit.Test;

import java.util.Properties;

/**
 * @since 3/18/15
 */
public class TestDataStorageLoader {
    @Test
    public void testDataStorage() throws IllegalDataStorageTypeException, IllegalDataStorageException {
        DataStorage dataStorage = DataStorageManager.newDataStorage("test");
        assert dataStorage instanceof TestDataStorage;

        // get eagle.storage.type (value: test) from src/test/resources/application.conf
        DataStorage dataStorage2 = DataStorageManager.getDataStorageByEagleConfig();
        assert dataStorage2 instanceof TestDataStorage;

        AbstractConfiguration configuration = new CombinedConfiguration();
        configuration.addProperty(DataStorageManager.EAGLE_STORAGE_TYPE,"test");
        DataStorage dataStorage3 = DataStorageManager.newDataStorage(configuration);
        assert dataStorage3 instanceof TestDataStorage;

        Properties properties = new Properties();
        properties.put(DataStorageManager.EAGLE_STORAGE_TYPE, "test");
        DataStorage dataStorage4 = DataStorageManager.newDataStorage(properties);
        assert dataStorage4 instanceof TestDataStorage;
    }
}