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

import org.apache.eagle.storage.DataStorage;
import org.junit.Test;

/**
 * @since 3/20/15
 */
public class TestDataStorageServiceProvider implements DataStorageServiceProvider {
    private static final String TEST = "test";

    @Override
    public String getType() {
        return TEST;
    }

    @Override
    public DataStorage getStorage() {
        return new TestDataStorage();
    }

    @Test
    public void test() {

    }
}