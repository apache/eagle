package org.apache.eagle.storage.operation;

import org.apache.eagle.log.entity.meta.EntityDefinitionManager;
import org.apache.eagle.log.entity.test.TestTimeSeriesAPIEntity;
import org.apache.eagle.storage.DataStorage;
import org.junit.Test;
import org.mockito.Mock;

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
