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
