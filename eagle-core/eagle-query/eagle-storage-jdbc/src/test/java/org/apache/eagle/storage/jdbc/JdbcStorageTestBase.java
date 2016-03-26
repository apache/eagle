package org.apache.eagle.storage.jdbc;

import org.apache.eagle.log.entity.meta.EntityDefinition;
import org.apache.eagle.log.entity.meta.EntityDefinitionManager;
import org.apache.eagle.log.entity.test.TestTimeSeriesAPIEntity;
import org.apache.eagle.storage.DataStorageManager;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.GregorianCalendar;
import java.util.TimeZone;

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
public class JdbcStorageTestBase {
    JdbcStorage storage;
    long baseTimestamp;
    final static Logger LOG = LoggerFactory.getLogger(TestJdbcStorage.class);

    @Before
    public void setUp() throws Exception {
        storage = (JdbcStorage) DataStorageManager.getDataStorageByEagleConfig();
        storage.init();
        GregorianCalendar gc = new GregorianCalendar();
        gc.clear();
        gc.set(2014, 1, 6, 1, 40, 12);
        gc.setTimeZone(TimeZone.getTimeZone("UTC"));
        baseTimestamp = gc.getTime().getTime();
        System.out.println("timestamp:" + baseTimestamp);
    }
}
