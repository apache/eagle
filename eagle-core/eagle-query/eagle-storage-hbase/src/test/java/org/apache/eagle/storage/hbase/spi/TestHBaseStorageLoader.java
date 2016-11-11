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
package org.apache.eagle.storage.hbase.spi;

import org.apache.eagle.service.hbase.TestHBaseBase;
import org.apache.eagle.storage.DataStorageManager;
import org.apache.eagle.storage.exception.IllegalDataStorageTypeException;
import org.apache.eagle.storage.hbase.HBaseStorage;
import org.junit.Assert;
import org.junit.Test;

/**
 * @since 3/20/15
 */
public class TestHBaseStorageLoader extends TestHBaseBase {
    @Test
    public void testHBaseStorageLoader() throws IllegalDataStorageTypeException {

        Assert.assertTrue(DataStorageManager.getDataStorageByEagleConfig() instanceof HBaseStorage);
        Assert.assertTrue(DataStorageManager.newDataStorage("hbase") instanceof HBaseStorage);

    }
}