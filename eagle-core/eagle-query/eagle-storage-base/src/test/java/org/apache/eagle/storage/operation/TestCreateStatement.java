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
package org.apache.eagle.storage.operation;

import org.apache.eagle.log.entity.meta.EntityDefinitionManager;
import org.apache.eagle.log.entity.test.TestTimeSeriesAPIEntity;
import org.apache.eagle.storage.DataStorage;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.mockito.Mockito.*;

/**
 * @Since 11/7/16.
 */
public class TestCreateStatement {

    private DataStorage mockDataStorage = mock(DataStorage.class);

    @Test(expected = IllegalArgumentException.class)
    public void testEntityNull() throws IOException {
        CreateStatement createStatement = new CreateStatement(null, TestTimeSeriesAPIEntity.class.getSimpleName());
        createStatement.execute(mockDataStorage);
    }

    @Test
    public void testCreateExecute() throws Exception {
        List<TestTimeSeriesAPIEntity> entities = new ArrayList<>();
        CreateStatement createStatement1 = new CreateStatement(entities, TestTimeSeriesAPIEntity.class.getSimpleName());
        CreateStatement createStatement2 = new CreateStatement(entities, EntityDefinitionManager.getEntityDefinitionByEntityClass(TestTimeSeriesAPIEntity.class));
        createStatement1.execute(mockDataStorage);
        createStatement2.execute(mockDataStorage);
        verify(mockDataStorage, times(2)).create(any(), any());
    }
}
