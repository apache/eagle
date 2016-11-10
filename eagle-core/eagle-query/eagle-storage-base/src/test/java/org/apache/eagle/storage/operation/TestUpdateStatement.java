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
public class TestUpdateStatement {

    private DataStorage mockDataStorage = mock(DataStorage.class);

    @Test
    public void testUpdateExecute() throws Exception {
        List<TestTimeSeriesAPIEntity> entities = new ArrayList<>();
        UpdateStatement updateStatement1 = new UpdateStatement(entities, TestTimeSeriesAPIEntity.class.getSimpleName());
        UpdateStatement updateStatement2 = new UpdateStatement(entities, EntityDefinitionManager.getEntityDefinitionByEntityClass(TestTimeSeriesAPIEntity.class));
        updateStatement1.execute(mockDataStorage);
        updateStatement2.execute(mockDataStorage);
        verify(mockDataStorage, times(2)).update(any(), any());
    }

}
